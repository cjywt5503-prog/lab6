"""Невеликий локальний журнал заявок для демонстрації n8n без зовнішніх акаунтів."""
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

DB_PATH = os.getenv('INCIDENT_DB_PATH', '/data/incidents.sqlite3')
app = FastAPI(title='BioLIMS навчальні інциденти')


@contextmanager
def connect():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('''CREATE TABLE IF NOT EXISTS incidents (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT NOT NULL UNIQUE,
        alertname TEXT NOT NULL, severity TEXT NOT NULL, target TEXT NOT NULL,
        summary TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL)''')
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


class Incident(BaseModel):
    fingerprint: str = Field(min_length=1, max_length=200)
    alertname: str = Field(min_length=1, max_length=200)
    severity: str = 'warning'
    target: str = ''
    summary: str = ''
    status: str = 'firing'


@app.post('/incidents')
def upsert_incident(incident: Incident):
    status = 'resolved' if incident.status == 'resolved' else 'open'
    with connect() as db:
        db.execute('''INSERT INTO incidents (fingerprint,alertname,severity,target,summary,status,updated_at)
            VALUES (?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET
            severity=excluded.severity, target=excluded.target, summary=excluded.summary,
            status=excluded.status, updated_at=excluded.updated_at''',
            (incident.fingerprint, incident.alertname, incident.severity, incident.target,
             incident.summary, status, datetime.now(timezone.utc).isoformat()))
        row = db.execute('SELECT * FROM incidents WHERE fingerprint=?', (incident.fingerprint,)).fetchone()
    return dict(row)


@app.get('/incidents')
def list_incidents():
    with connect() as db:
        return [dict(row) for row in db.execute('SELECT * FROM incidents ORDER BY id DESC')]


@app.get('/', response_class=HTMLResponse)
def index():
    # textContent avoids rendering alert text as HTML.
    return '''<!doctype html><html lang="uk"><head><meta charset="utf-8"><title>BioLIMS — інциденти</title>
<style>body{font:16px system-ui;margin:40px;max-width:1100px}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ccc;padding:9px;text-align:left}th{background:#e8eff5}</style></head>
<body><h1>Навчальні інциденти BioLIMS</h1><p>Заявки, створені n8n з повідомлень Alertmanager. Дані оновлюються кожні 5 секунд.</p>
<table><thead><tr><th>№</th><th>Подія</th><th>Вузол</th><th>Важливість</th><th>Стан</th><th>Час UTC</th></tr></thead><tbody id="rows"></tbody></table>
<script>async function load(){const r=await fetch('/incidents');const data=await r.json();const rows=document.querySelector('#rows');rows.replaceChildren();for(const i of data){const tr=document.createElement('tr');for(const k of ['id','alertname','target','severity','status','updated_at']){const td=document.createElement('td');td.textContent=i[k];tr.appendChild(td)}rows.appendChild(tr)}}load();setInterval(load,5000)</script></body></html>'''

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8005)
