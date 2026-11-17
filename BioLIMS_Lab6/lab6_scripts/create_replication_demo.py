"""Створює унікальний запис у первинному шарді для демонстрації реплікації."""
import hashlib
import os
from uuid import uuid4
import requests

shard=int(os.getenv('SHARD_ID','0'))
while True:
    barcode='DEMO-'+uuid4().hex[:12]
    if int.from_bytes(hashlib.sha256(barcode.encode()).digest()[:8],'big')%2==shard:
        break
order={'order_id':'ORD-'+uuid4().hex,'patient_id':'PAT-DEMO','barcode':barcode,
       'biomarkers':['GLU'],'status':'SAMPLE_COLLECTED'}
r=requests.post('http://127.0.0.1:8004/internal/orders',json=order,timeout=5)
r.raise_for_status()
print('Створено навчальне замовлення:', barcode, '| шард:', shard)
print('Результат:',r.json())
