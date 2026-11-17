"""Навчальний шард: SQLite primary і асинхронна репліка через окремий HTTP вузол."""
import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager

import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request

from common.contracts import OrderData
from common.observability import configure_observability, log_event, outbound_traceparent, trace_context
from common.storage import database

SHARD = int(os.getenv('SHARD_ID', '0'))
ROLE = os.getenv('SHARD_ROLE', 'primary')
SERVICE_NAME = os.getenv('SERVICE_NAME', f'shard-{SHARD}-{ROLE}')
DB_PATH = os.getenv('SHARD_DB_PATH', '/data/shard.sqlite3')
REPLICA_URL = os.getenv('REPLICA_URL', f'http://shard-{SHARD}-replica:8004')
SHARD_COUNT = 2
SEEDS = [
    {'order_id': 'ORD-001', 'patient_id': 'PAT-409', 'barcode': 'CODE128-1001',
     'biomarkers': ['GLU', 'ALT', 'CREA'], 'status': 'SAMPLE_COLLECTED'},
    {'order_id': 'ORD-002', 'patient_id': 'PAT-DEMO', 'barcode': 'CODE128-1002',
     'biomarkers': ['GLU', 'ALT'], 'status': 'SAMPLE_COLLECTED'},
    {'order_id': 'ORD-DEMO-ERROR', 'patient_id': 'PAT-TEST', 'barcode': 'CODE128-ERROR',
     'biomarkers': ['GLU', 'ALT', 'CREA'], 'status': 'SAMPLE_COLLECTED'},
]


def shard_for(barcode):
    return int.from_bytes(hashlib.sha256(barcode.encode('utf-8')).digest()[:8], 'big') % SHARD_COUNT


def insert_order(order, traceparent=None):
    if ROLE != 'primary':
        raise HTTPException(403, 'Записи дозволені лише на primary')
    if shard_for(order['barcode']) != SHARD:
        raise HTTPException(400, 'Це замовлення належить іншому шарду')
    payload = json.dumps(order, ensure_ascii=False)
    with database(DB_PATH, write=True) as db:
        db.execute('INSERT INTO orders (barcode, payload) VALUES (?, ?)', (order['barcode'], payload))
        db.execute('INSERT INTO replication_queue (barcode, payload, created_at, traceparent) VALUES (?, ?, ?, ?)',
                   (order['barcode'], payload, time.time(), traceparent))


def init_db():
    with database(DB_PATH, write=True) as db:
        db.execute('CREATE TABLE IF NOT EXISTS orders (barcode TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.execute('''CREATE TABLE IF NOT EXISTS replication_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT, barcode TEXT NOT NULL, payload TEXT NOT NULL,
            created_at REAL NOT NULL, traceparent TEXT, replicated_at REAL)''')
    if ROLE == 'primary':
        for order in SEEDS:
            if shard_for(order['barcode']) != SHARD:
                continue
            try:
                insert_order(order)
            except sqlite3.IntegrityError:
                pass


def replicate_once():
    with database(DB_PATH) as db:
        row = db.execute('SELECT * FROM replication_queue WHERE replicated_at IS NULL ORDER BY id LIMIT 1').fetchone()
    if not row:
        return False
    row = dict(row)
    with trace_context(row['traceparent']):
        response = requests.post(f'{REPLICA_URL}/internal/replicate',
                                 json={'order': json.loads(row['payload'])},
                                 headers={'traceparent': outbound_traceparent()}, timeout=2)
        response.raise_for_status()
        with database(DB_PATH, write=True) as db:
            db.execute('UPDATE replication_queue SET replicated_at=? WHERE id=?', (time.time(), row['id']))
        log_event('replication.confirmed', shard=SHARD, queue_id=row['id'])
    return True


def replicate_forever(stop):
    while not stop.is_set():
        try:
            if not replicate_once():
                stop.wait(1)
        except (requests.RequestException, sqlite3.Error, ValueError):
            # Черга зберігається на primary і буде повторена після відновлення replica.
            log_event('replication.retry', shard=SHARD)
            stop.wait(2)


def shard_metrics():
    with database(DB_PATH) as db:
        count = db.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
        row = db.execute('SELECT COUNT(*), MIN(created_at) FROM replication_queue WHERE replicated_at IS NULL').fetchone()
    pending, oldest = row[0], row[1]
    lag = max(0, time.time() - oldest) if oldest is not None and ROLE == 'primary' else 0
    label = f'shard="{SHARD}",role="{ROLE}"'
    return (f'# HELP biolims_shard_orders Number of orders in a shard node\n'
            f'# TYPE biolims_shard_orders gauge\nbiolims_shard_orders{{{label}}} {count}\n'
            f'# HELP biolims_replication_pending Number of unreplicated operations\n'
            f'# TYPE biolims_replication_pending gauge\nbiolims_replication_pending{{{label}}} {pending}\n'
            f'# HELP biolims_replication_lag_seconds Age of the oldest unreplicated write, zero if none\n'
            f'# TYPE biolims_replication_lag_seconds gauge\nbiolims_replication_lag_seconds{{{label}}} {lag:.3f}\n')


@asynccontextmanager
async def lifespan(app):
    init_db()
    stop = threading.Event()
    thread = None
    if ROLE == 'primary':
        thread = threading.Thread(target=replicate_forever, args=(stop,), daemon=True)
        thread.start()
    try:
        yield
    finally:
        stop.set()
        if thread:
            thread.join(timeout=4)


app = FastAPI(title='BioLIMS навчальний шард', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

@app.get('/health')
def health():
    with database(DB_PATH) as db:
        db.execute('SELECT 1 FROM orders LIMIT 1')
    return {'status': 'UP', 'shard': SHARD, 'role': ROLE}

@app.get('/internal/orders')
def all_orders():
    with database(DB_PATH) as db:
        return [json.loads(row['payload']) for row in db.execute('SELECT payload FROM orders ORDER BY barcode')]

@app.get('/internal/orders/{barcode}')
def get_order(barcode: str):
    with database(DB_PATH) as db:
        row = db.execute('SELECT payload FROM orders WHERE barcode=?', (barcode,)).fetchone()
    if row is None:
        raise HTTPException(404, 'Замовлення не знайдено')
    return json.loads(row['payload'])

@app.post('/internal/orders', status_code=201)
def create_order(order: OrderData):
    try:
        insert_order(order.model_dump(), outbound_traceparent())
    except sqlite3.IntegrityError as exc:
        raise HTTPException(409, 'Замовлення з таким штрихкодом уже існує') from exc
    return {'status': 'created', 'shard': SHARD}

@app.post('/internal/replicate')
def replicate(body: dict):
    if ROLE != 'replica':
        raise HTTPException(403, 'Цей вузол не є реплікою')
    order = OrderData.model_validate(body['order']).model_dump()
    if shard_for(order['barcode']) != SHARD:
        raise HTTPException(400, 'Невідповідний шард')
    with database(DB_PATH, write=True) as db:
        db.execute('''INSERT INTO orders (barcode, payload) VALUES (?, ?)
            ON CONFLICT(barcode) DO UPDATE SET payload=excluded.payload''',
                   (order['barcode'], json.dumps(order, ensure_ascii=False)))
    log_event('replication.applied', shard=SHARD)
    return {'status': 'applied', 'shard': SHARD}

@app.get('/lab6/demo')
def demo(delay: float = 0):
    if not 0 <= delay <= 5:
        raise HTTPException(400, 'delay must be between 0 and 5 seconds')
    time.sleep(delay)
    return {'status': 'ok', 'shard': SHARD, 'delay': delay}

configure_observability(app, SERVICE_NAME, shard_metrics)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8004)
