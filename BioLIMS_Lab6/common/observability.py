"""Small, dependency-free Prometheus and W3C Trace Context implementation for the lab.

A process handles one uvicorn worker. Traces are correlated in JSON logs; this
module does not export spans to a tracing backend.
"""
import contextlib
import contextvars
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

from fastapi import Request
from fastapi.responses import Response

_TRACEPARENT = re.compile(r'^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$')
_current = contextvars.ContextVar('lab6_trace', default=None)
_log = logging.getLogger('biolims.observability')
_log.setLevel(logging.INFO)
_log.propagate = False
if not _log.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(message)s'))
    _log.addHandler(handler)


def new_context(header=None):
    match = _TRACEPARENT.fullmatch((header or '').lower())
    if match and int(match.group(1), 16) and int(match.group(2), 16):
        trace_id, parent_id, flags = match.groups()
    else:
        trace_id, parent_id, flags = uuid.uuid4().hex, None, '01'
    return {'trace_id': trace_id, 'span_id': uuid.uuid4().hex[:16],
            'parent_span_id': parent_id, 'flags': flags}


@contextlib.contextmanager
def trace_context(header=None):
    token = _current.set(new_context(header))
    try:
        yield _current.get()
    finally:
        _current.reset(token)


def outbound_traceparent():
    c = _current.get()
    if not c:
        c = new_context()
    return f"00-{c['trace_id']}-{c['span_id']}-{c['flags']}"


def trace_headers(headers=None):
    return {**(headers or {}), 'traceparent': outbound_traceparent()}


def log_event(event, **fields):
    c = _current.get() or {}
    _log.info(json.dumps({'timestamp': datetime.now(timezone.utc).isoformat(),
        'service': os.getenv('SERVICE_NAME', 'biolims'), 'event': event,
        'trace_id': c.get('trace_id'), 'span_id': c.get('span_id'),
        'parent_span_id': c.get('parent_span_id'), **fields}, ensure_ascii=False))


class Metrics:
    BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(self, service):
        self.service = service
        self.lock = threading.Lock()
        self.series = {}

    def observe(self, method, status, elapsed):
        key = (method, str(status))
        with self.lock:
            data = self.series.setdefault(key, {'count': 0, 'sum': 0.0,
                                                'buckets': [0] * len(self.BUCKETS)})
            data['count'] += 1
            data['sum'] += elapsed
            for i, limit in enumerate(self.BUCKETS):
                if elapsed <= limit:
                    data['buckets'][i] += 1

    @staticmethod
    def label(value):
        return str(value).replace('\\', '\\\\').replace('\n', '\\n').replace('"', '\\"')

    def render(self):
        counters = ['# HELP biolims_http_requests_total Number of HTTP requests',
                    '# TYPE biolims_http_requests_total counter']
        histograms = ['# HELP biolims_http_request_duration_seconds HTTP response time',
                      '# TYPE biolims_http_request_duration_seconds histogram']
        with self.lock:
            for (method, status), row in sorted(self.series.items()):
                labels = f'service="{self.label(self.service)}",method="{method}",status="{status}"'
                counters.append(f"biolims_http_requests_total{{{labels}}} {row['count']}")
                for limit, count in zip(self.BUCKETS, row['buckets']):
                    histograms.append(f'biolims_http_request_duration_seconds_bucket{{{labels},le="{limit}"}} {count}')
                histograms.append(f"biolims_http_request_duration_seconds_bucket{{{labels},le=\"+Inf\"}} {row['count']}")
                histograms.append(f"biolims_http_request_duration_seconds_sum{{{labels}}} {row['sum']}")
                histograms.append(f"biolims_http_request_duration_seconds_count{{{labels}}} {row['count']}")
        lines = counters + histograms
        lines.extend(['# HELP biolims_process_cpu_seconds_total CPU time of this process',
                      '# TYPE biolims_process_cpu_seconds_total counter',
                      f'biolims_process_cpu_seconds_total {time.process_time()}'])
        try:
            with open('/proc/self/statm', encoding='ascii') as file:
                resident = int(file.read().split()[1]) * os.sysconf('SC_PAGE_SIZE')
            lines.extend(['# HELP biolims_process_resident_memory_bytes Resident memory',
                          '# TYPE biolims_process_resident_memory_bytes gauge',
                          f'biolims_process_resident_memory_bytes {resident}'])
        except (OSError, ValueError, IndexError):
            pass
        return '\n'.join(lines) + '\n'



def configure_observability(app, service_name, extra_metrics=None):
    metrics = Metrics(service_name)
    app.state.lab6_metrics = metrics

    @app.get('/metrics', include_in_schema=False)
    def metrics_endpoint():
        extra = extra_metrics() if extra_metrics else ''
        return Response(metrics.render() + extra, media_type='text/plain; version=0.0.4')

    @app.middleware('http')
    async def collect_and_log(request: Request, call_next):
        # Scrapes and readiness probes must not inflate request rates or latency.
        if request.url.path in ('/metrics', '/health'):
            return await call_next(request)
        started = time.perf_counter()
        status = 500
        with trace_context(request.headers.get('traceparent')) as context:
            try:
                response = await call_next(request)
                status = response.status_code
                response.headers['X-Trace-ID'] = context['trace_id']
                response.headers['traceparent'] = outbound_traceparent()
                return response
            finally:
                elapsed = time.perf_counter() - started
                metrics.observe(request.method, status, elapsed)
                route = request.scope.get('route')
                log_event('http.request', method=request.method,
                          route=getattr(route, 'path', 'unmatched'), status=status,
                          duration_ms=round(elapsed * 1000, 3))
