"""Генерує навчальні HTTP-запити для графіків Prometheus."""
import argparse
import time
import requests

p=argparse.ArgumentParser()
p.add_argument('--slow',action='store_true',help='Викликати затримку для сповіщення p99')
args=p.parse_args()
targets=['http://127.0.0.1:8001/lab6/demo','http://shard-0-primary:8004/lab6/demo',
         'http://shard-1-primary:8004/lab6/demo']
for target in targets:
    for _ in range(25):
        response=requests.get(target,timeout=5);response.raise_for_status()
    print('25 запитів:',target)
if args.slow:
    for _ in range(12):
        response=requests.get(targets[0],params={'delay':0.8},timeout=5);response.raise_for_status()
    print('Виконано 12 повільних запитів до координатора.')
print('Зачекайте приблизно 30–60 секунд і оновіть Grafana / Prometheus.')
