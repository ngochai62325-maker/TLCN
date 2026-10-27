"""Bounded final publication read window during host CSV-directory promotion.

Reuses Gold/source/Analytics locks. No persistent state, SQL writes or new service.
"""
from contextlib import contextmanager
import hashlib
import json
import queue
import subprocess
import sys
import tempfile
import threading
import time

from analytics import VERSION
from analytics.client import windows_available_bytes
from analytics.queries import compile_query

OPERATIONS = {'publication', 'fact_overview', 'nso_scope_catalogue', 'nso_observations'}


def valid_finish(line):
    # Windows Popen text pipes emit CRLF even when the host writes '\n'.
    return line in ('FINISH\n', 'FINISH\r\n')


def check_manifest(manifest, payload):
    from gold.admission import code_fingerprint
    from gold.keys import fingerprint
    pins = {n: {k: v[k] for k in ('uuid', 'snapshot_id')} for n, v in manifest['outputs'].items()}
    if (manifest['batch'] != payload['batch'] or fingerprint(manifest) != payload['manifest_sha256']
            or pins != payload['snapshots'] or code_fingerprint() != manifest['context']['code_fingerprint']):
        raise RuntimeError('Publication/schema/snapshot fingerprint changed during export')
    if not 1 <= len(payload['definitions']) <= 32:
        raise ValueError('Invalid export definition inventory')
    for record in payload['definitions']:
        if record['query'] not in OPERATIONS or record['version'] != VERSION:
            raise ValueError('Export operation/version outside allowlist')
        sql, definition = compile_query(record['query'], record['parameters'], manifest)
        if (definition != record['definition_sha256'] or hashlib.sha256((sql or '').encode()).hexdigest() != record['compiled_sql_sha256']):
            raise RuntimeError('Analytics SQL definition changed during export')


def serve():
    """Internal runner protocol: LOCKED -> host promotion -> FINISH -> VERIFIED."""
    import select
    from analytics.access import admission
    from gold.consumer import read_window
    raw = sys.stdin.readline(65537)
    if len(raw.encode('utf-8')) > 65536:
        raise ValueError('Oversized export guard request')
    payload = json.loads(raw)
    if set(payload) != {'batch', 'manifest_sha256', 'snapshots', 'definitions', 'admission'}:
        raise ValueError('Invalid export guard envelope')
    with admission(payload['admission']), read_window() as manifest:
        check_manifest(manifest, payload)
        print(json.dumps({'status': 'LOCKED', 'batch': manifest['batch'], 'manifest_sha256': payload['manifest_sha256']}), flush=True)
        if not select.select([sys.stdin], [], [], 20)[0] or not valid_finish(sys.stdin.readline(16)):
            raise RuntimeError('Export guard timeout/disconnection')
        # Re-read physical metadata and publication before acknowledging. Outer
        # read/source locks stay held until the nested window and protocol finish.
        with read_window() as readback:
            check_manifest(readback, payload)
        print(json.dumps({'status': 'VERIFIED', 'batch': manifest['batch'], 'manifest_sha256': payload['manifest_sha256']}), flush=True)


def read_message(process, timeout=40):
    messages = queue.Queue(maxsize=1)
    def read():
        try:
            messages.put(process.stdout.readline(65537))
        except Exception:
            messages.put('')
    threading.Thread(target=read, daemon=True).start()
    try:
        line = messages.get(timeout=timeout)
    except queue.Empty:
        raise TimeoutError('Publication promotion guard timed out') from None
    if not line or len(line.encode('utf-8')) > 65536:
        raise RuntimeError('Publication guard failed; no export accepted')
    return json.loads(line)


@contextmanager
def publication_fence(payload):
    memory = windows_available_bytes()
    if memory < 2*1024**3:
        raise RuntimeError('RESOURCE_BLOCKED: Windows available <2 GiB')
    request = dict(payload, admission={'windows_available_bytes': memory, 'measured_at': time.time()})
    command = ['docker', 'exec', '-i', 'tlcn-local-runner-1', 'python3',
               '/workspace/jobs/analytics/export_powerbi.py', '--_hold-publication']
    with tempfile.TemporaryFile(mode='w+', encoding='utf-8') as errors:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors,
                                   text=True, encoding='utf-8')
        try:
            process.stdin.write(json.dumps(request)+'\n'); process.stdin.flush()
            ready = read_message(process)
            expected = dict(batch=payload['batch'], manifest_sha256=payload['manifest_sha256'])
            if ready != dict(expected, status='LOCKED') or process.poll() is not None:
                raise RuntimeError('Publication guard readiness mismatch')
            yield
            process.stdin.write('FINISH\n'); process.stdin.flush()
            if read_message(process) != dict(expected, status='VERIFIED'):
                raise RuntimeError('Publication guard readback mismatch')
            if process.wait(timeout=5) != 0:
                raise RuntimeError('Publication guard release failed')
        finally:
            if process.poll() is None:
                process.kill(); process.wait(timeout=5)
            process.stdin.close(); process.stdout.close()
