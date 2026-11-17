"""Координатор замовлень: автентифікація, маршрутизація за штрихкодом."""
import hashlib
import os
import socket
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import requests
import uvicorn
from fastapi import FastAPI, HTTPException

from common.consul import ServiceRegistration
from common.contracts import CreateOrderRequest
from common.observability import configure_observability, trace_headers
from common.security import protect

INSTANCE_ID = socket.gethostname()
SERVICE_NAME = os.getenv('SERVICE_NAME', 'order-sample-service')
SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8001'))
SHARDS = [os.getenv('SHARD_0_URL', 'http://shard-0-primary:8004'),
          os.getenv('SHARD_1_URL', 'http://shard-1-primary:8004')]


def shard_for(barcode):
    return int.from_bytes(hashlib.sha256(barcode.encode('utf-8')).digest()[:8], 'big') % len(SHARDS)


def shard_request(method, shard, path, **kwargs):
    try:
        response = requests.request(method, SHARDS[shard] + path, headers=trace_headers(), timeout=5, **kwargs)
    except requests.RequestException as exc:
        raise HTTPException(503, f'Шард {shard} недоступний') from exc
    if response.status_code in (404, 409):
        raise HTTPException(response.status_code, response.json().get('detail', 'Помилка шарда'))
    if not response.ok:
        raise HTTPException(502, f'Шард {shard} повернув помилку')
    return response.json()


@asynccontextmanager
async def lifespan(app):
    registration = ServiceRegistration(SERVICE_NAME, SERVICE_PORT, INSTANCE_ID)
    registration.start()
    try:
        yield
    finally:
        registration.stop()


app = FastAPI(title='Order Coordinator', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
protect(app, lambda request: 'orders.read' if request.method == 'GET' else 'orders.write' if request.method == 'POST' else None)

@app.get('/health')
def health():
    return {'status': 'UP', 'instance': INSTANCE_ID, 'role': 'coordinator'}

@app.get('/orders')
def list_orders():
    orders = []
    for shard in range(len(SHARDS)):
        orders.extend(shard_request('GET', shard, '/internal/orders'))
    return {'service': SERVICE_NAME, 'handled_by_instance': INSTANCE_ID,
            'orders_count': len(orders), 'orders': sorted(orders, key=lambda x: x['barcode'])}

@app.post('/orders', status_code=201)
def create_order(req: CreateOrderRequest):
    order = {'order_id': f'ORD-{uuid4()}', **req.model_dump(), 'status': 'SAMPLE_COLLECTED'}
    shard = shard_for(req.barcode)
    shard_request('POST', shard, '/internal/orders', json=order)
    return {'message': 'Замовлення створено та штрихкод нанесено на пробірку',
            'handled_by_instance': INSTANCE_ID, 'shard': shard, 'order': order}

@app.get('/orders/by-barcode/{barcode}')
def get_order_by_barcode(barcode: str, client_note: str = 'Init'):
    shard = shard_for(barcode)
    order = shard_request('GET', shard, '/internal/orders/' + barcode)
    return {'order': order, 'handled_by_order_instance': INSTANCE_ID, 'shard': shard,
            'trace_chain': f"{client_note} -> [1. OrderCoordinator: shard-{shard}, instance: {INSTANCE_ID}]"}

@app.get('/lab6/demo')
def demo(delay: float = 0):
    if not 0 <= delay <= 5:
        raise HTTPException(400, 'delay must be between 0 and 5 seconds')
    time.sleep(delay)
    return {'status': 'ok', 'delay': delay}

configure_observability(app, SERVICE_NAME)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=SERVICE_PORT)
