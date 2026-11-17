"""Перевірки із SQLite та справжніми ASGI-запитами; мережа між вузлами замінена TestClient."""
import importlib.util
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from common import security
from common.observability import Metrics, new_context, outbound_traceparent, trace_context

ROOT = Path(__file__).resolve().parents[1]


def load(path, **env):
    with patch.dict(os.environ, env):
        spec = importlib.util.spec_from_file_location('lab6_' + uuid4().hex, ROOT / path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class Lab6Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.primary = []
        self.replica = []
        for i in range(2):
            for role, target in [('primary', self.primary), ('replica', self.replica)]:
                m = load('shard-service/app.py', SHARD_ID=str(i), SHARD_ROLE=role,
                         SHARD_DB_PATH=str(Path(self.tmp.name) / f'{i}-{role}.db'))
                m.init_db()
                target.append(m)
        self.clients = [TestClient(m.app) for m in self.primary]
        self.replica_clients = [TestClient(m.app) for m in self.replica]
        self.coordinator = load('order-sample-service/app.py')
        self.order_client = TestClient(self.coordinator.app)

    def routed(self, method, url, headers=None, timeout=None, **kwargs):
        for i, target in enumerate(self.coordinator.SHARDS):
            if url.startswith(target):
                response = self.clients[i].request(method, url[len(target):], headers=headers, **kwargs)
                return SimpleNamespace(status_code=response.status_code,
                                       ok=response.is_success, json=response.json)
        raise AssertionError('Unexpected URL: ' + url)

    def test_w3c_trace_parent_validation(self):
        incoming='00-'+'1234567890abcdef'*2+'-'+'23456789abcdef01'+'-01'
        ctx=new_context(incoming)
        self.assertEqual(ctx['trace_id'], '1234567890abcdef'*2)
        self.assertEqual(ctx['parent_span_id'], '23456789abcdef01')
        with trace_context(incoming) as local:
            self.assertEqual(outbound_traceparent().split('-')[1], local['trace_id'])
            self.assertEqual(outbound_traceparent().split('-')[2], local['span_id'])
        self.assertNotEqual(new_context('00-'+'0'*32+'-'+'0'*16+'-01')['trace_id'], '0'*32)

    def test_histogram_cumulative_buckets(self):
        m=Metrics('orders'); m.observe('GET',200,0.2); m.observe('GET',500,0.7)
        output=m.render()
        self.assertIn('biolims_http_requests_total{service="orders",method="GET",status="200"} 1',output)
        self.assertIn('status="200",le="0.25"} 1',output)
        self.assertIn('status="500",le="0.5"} 0',output)
        self.assertIn('status="500",le="1.0"} 1',output)

    def test_coordinator_routes_and_preserves_trace(self):
        incoming='00-'+'abcdef0123456789'*2+'-'+'1234567890abcdef'+'-01'
        with patch.object(security,'validate_access_token',return_value={}), \
             patch.object(self.coordinator.requests,'request',side_effect=self.routed):
            a=self.order_client.get('/orders/by-barcode/CODE128-1001',headers={'Authorization':'Bearer demo','traceparent':incoming})
            b=self.order_client.get('/orders/by-barcode/CODE128-1002',headers={'Authorization':'Bearer demo','traceparent':incoming})
        self.assertEqual(a.status_code,200); self.assertEqual(b.status_code,200)
        self.assertNotEqual(a.json()['shard'],b.json()['shard'])
        self.assertEqual(a.headers['x-trace-id'],incoming.split('-')[1])
        self.assertEqual(b.headers['x-trace-id'],incoming.split('-')[1])
        shard_metrics=self.clients[b.json()['shard']].get('/metrics').text
        self.assertIn('biolims_http_requests_total',shard_metrics)

    def test_replication_lag_and_acknowledgment(self):
        primary=self.primary[0]
        response=self.clients[0].post('/internal/orders',json={
            'order_id':'ORD-TEST','patient_id':'PAT-TEST','barcode':'LAB6-0001',
            'biomarkers':['GLU'],'status':'SAMPLE_COLLECTED'})
        self.assertEqual(response.status_code,201)
        self.assertEqual(self.replica_clients[0].get('/internal/orders/LAB6-0001').status_code,404)
        with patch.object(primary.requests,'post',side_effect=ConnectionError('offline')):
            with self.assertRaises(ConnectionError): primary.replicate_once()
        self.assertIn('biolims_replication_pending{shard="0",role="primary"} 2', primary.shard_metrics())
        def replicate(url, json, headers, timeout):
            return self.replica_clients[0].post('/internal/replicate',json=json,headers=headers)
        with patch.object(primary.requests,'post',side_effect=replicate):
            while primary.replicate_once(): pass
        self.assertEqual(self.replica_clients[0].get('/internal/orders/LAB6-0001').status_code,200)
        self.assertIn('biolims_replication_pending{shard="0",role="primary"} 0', primary.shard_metrics())
        self.assertIn('biolims_replication_lag_seconds{shard="0",role="primary"} 0.000',primary.shard_metrics())

    def test_assay_saga_preserves_trace_across_http_and_queue(self):
        from common import messaging
        producer=load('assay-execution-service/app.py', ASSAY_DB_PATH=str(Path(self.tmp.name)/'assay.db'))
        consumer=load('validation-report-service/app.py', REPORT_DB_PATH=str(Path(self.tmp.name)/'report.db'))
        producer.init_db(); consumer.init_db()
        def coordinator_get(url, params, headers, timeout):
            response=self.order_client.get(url.split(':8001',1)[1],params=params,headers=headers)
            return SimpleNamespace(status_code=response.status_code,json=response.json)
        incoming='00-'+'abcdef0123456789'*2+'-'+'1234567890abcdef'+'-01'
        with patch.object(security,'validate_access_token',return_value={}), \
             patch.object(producer.discovery,'candidates',return_value=['coordinator:8001']), \
             patch.object(producer.requests,'get',side_effect=coordinator_get), \
             patch.object(self.coordinator.requests,'request',side_effect=self.routed):
            result=TestClient(producer.app).post('/assays/run',
                headers={'Authorization':'Bearer demo','traceparent':incoming},json={'barcode':'CODE128-1001'})
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.headers['x-trace-id'],incoming.split('-')[1])
        with producer.database(producer.DB_PATH) as db:
            raw=db.execute('SELECT payload_json FROM outbox_messages').fetchone()[0]
        event=json.loads(raw)
        self.assertEqual(event['traceparent'].split('-')[1],incoming.split('-')[1])
        channel=Mock(); method=SimpleNamespace(delivery_tag=1)
        consumer.on_message(channel,method,None,raw.encode())
        channel.basic_ack.assert_called_once()
        sent=[]
        messaging.relay_once(consumer.DB_PATH,lambda queue,body,message_id:sent.append((queue,body)))
        self.assertEqual(json.loads(sent[0][1])['traceparent'].split('-')[1],incoming.split('-')[1])
        success=Mock();producer.on_success(success,method,None,sent[0][1].encode())
        success.basic_ack.assert_called_once()
        self.assertEqual(producer.get_assay(result.json()['assay_id'])['saga_status'],'COMPLETED_SUCCESSFULLY')
        self.assertEqual(consumer.get_report('CODE128-1001')['assay_id'],result.json()['assay_id'])

    def test_incident_deduplication_and_resolution(self):
        module=load('incident-service/app.py',INCIDENT_DB_PATH=str(Path(self.tmp.name)/'incidents.db'))
        client=TestClient(module.app)
        item={'fingerprint':'abc','alertname':'BioLIMSNodeDown','target':'shard-0','status':'firing'}
        self.assertEqual(client.post('/incidents',json=item).status_code,200)
        self.assertEqual(client.post('/incidents',json=item).status_code,200)
        item['status']='resolved';client.post('/incidents',json=item)
        rows=client.get('/incidents').json()
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['status'],'resolved')

if __name__ == '__main__':
    unittest.main()
