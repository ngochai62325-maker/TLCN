import contextlib
import io
import json
from decimal import Decimal
from unittest.mock import patch

import pytest

from analytics.queries import validate, compile_query, PERIODS
from analytics.transport import execute


def manifest():
    from gold.spec import load_spec
    return {"outputs": {n:{"uuid":"verified", "snapshot_id":"123"} for n in load_spec()}}


@pytest.mark.parametrize("name,params", [
    ("unknown",{}), ("catalogue",{"kind":"iceberg.silver_local"}),
    ("fact_overview",{"fact":"fact_nso_observation;DROP"}),
    ("publication",{"sql":"SELECT 1"}),
    ("nso_observations",dict(source_table="V06.12.csv",start_year=True,end_year=2024,level="national")),
    ("trade",dict(reporter="a"*64,product="b"*64,flow="export",start_year=1986,end_year=2024)),
    ("trade",dict(reporter="bad",product="b"*64,flow="export",start_year=2020,end_year=2024)),
    ("price_observations",dict(fact="fact_fao_trade_observation",series="a"*64,start_period=2020,end_period=2024)),
])
def test_invalid_parameters_fail_before_query(name,params):
    with pytest.raises(ValueError):
        validate(name,params)


def test_semantic_filtering_precedes_pivot_and_binds_all_snapshots():
    p=dict(reporter="a"*64,product="b"*64,flow="export",start_year=2020,end_year=2024)
    sql,h=compile_query("trade",p,manifest())
    assert "FOR VERSION AS OF 123" in sql and "{{" not in sql
    assert "WHERE year_key BETWEEN 2020 AND 2024 AND product_key" in sql
    assert "reporter_geography_key='"+"a"*64+"'" in sql
    assert "('5910', '5922')" in sql and "LIMIT 5001" in sql
    assert h==compile_query("trade",p,manifest())[1]
    broken=manifest();broken['outputs']['fact_fao_trade_observation']['snapshot_id']='0'
    with pytest.raises(ValueError):compile_query("trade",p,broken)


@pytest.mark.parametrize("fact",PERIODS)
def test_overview_is_full_scope_aggregate_not_sample(fact):
    sql,_=compile_query("fact_overview",{"fact":fact},manifest())
    assert "count(value)" in sql and sql.count("LIMIT")==1


def pages(*items):
    calls=[]
    def open_page(request,timeout):
        calls.append(request)
        if request.get_method()=='DELETE':return io.BytesIO(b'')
        return io.BytesIO(json.dumps(items[len([c for c in calls if c.get_method()!='DELETE'])-1]).encode())
    return open_page,calls


def test_exact_decimal_and_empty_result():
    opener,_=pages({"columns":[{"name":"value"}],"data":[["123456789.12345678"]],"id":"q"})
    assert execute("SELECT 1",opener=opener)['rows']==[["123456789.12345678"]]
    opener,_=pages({"id":"empty"})
    assert execute("SELECT 1",opener=opener)['rows']==[]


def test_overflow_cancels_instead_of_sampling():
    opener,calls=pages({"id":"q","data":[[1]]*5001,"nextUri":"http://trino:8080/v1/statement/q/next"})
    with pytest.raises(RuntimeError,match="overflow"):execute("SELECT 1",opener=opener)
    assert calls[-1].method=='DELETE'


def test_query_failure_and_foreign_polling_host_fail_closed():
    opener,_=pages({"id":"q","error":{"message":"failed SQL"}})
    with pytest.raises(RuntimeError,match="failed SQL"):execute("SELECT 1",opener=opener)
    opener,_=pages({"nextUri":"http://foreign:8080/x"})
    with pytest.raises(RuntimeError,match="polling"):execute("SELECT 1",opener=opener)


def test_time_budget():
    opener,_=pages({"id":"q"})
    times=iter([0,16])
    with pytest.raises(TimeoutError):execute("SELECT 1",opener=opener,clock=lambda:next(times))


def test_byte_budget_and_decimal_json_precision():
    with pytest.raises(RuntimeError,match='byte budget'):
        execute('SELECT 1',opener=lambda *a,**k:io.BytesIO(b'x'*(5*1024*1024+1)))
    body=b'{"columns":[{"name":"v"}],"data":[[123456789.12345678]],"id":"q"}'
    assert execute('SELECT 1',opener=lambda *a,**k:io.BytesIO(body))['rows'][0][0]==Decimal('123456789.12345678')


def test_invalid_date_and_low_memory_rejected():
    from analytics.access import admission
    import time
    with pytest.raises(ValueError):validate('price_observations',dict(fact='fact_price_monthly',series='a'*64,start_period=202413,end_period=202501))
    with pytest.raises(RuntimeError,match='resource admission'):
        with admission({'windows_available_bytes':1,'measured_at':time.time()}):pass


def test_unpublished_and_fingerprint_mismatch_never_execute():
    from analytics.access import run_query
    @contextlib.contextmanager
    def admitted(*args):yield
    @contextlib.contextmanager
    def unpublished():raise RuntimeError("unpublished");yield
    with patch('analytics.access.admission',admitted),patch('gold.consumer.read_window',unpublished),patch('analytics.access.execute') as sql:
        with pytest.raises(RuntimeError,match='unpublished'):run_query('publication',{},None)
        sql.assert_not_called()


def test_no_cache_gate_is_entered_on_every_request():
    from analytics.access import run_query
    from gold.keys import fingerprint
    m={**manifest(),"batch":"batch", "context":{"code_fingerprint":"code"}}
    for v in m['outputs'].values():v['rows']=1
    @contextlib.contextmanager
    def admitted(*args):yield
    @contextlib.contextmanager
    def window():yield m
    with patch('analytics.access.admission',admitted),patch('gold.consumer.read_window',side_effect=window) as gate,patch('gold.admission.code_fingerprint',return_value='code'):
        for _ in range(2):assert run_query('publication',{},dict(windows_available_bytes=3*1024**3))['cache']=='DISABLED'
        assert gate.call_count==2
        with pytest.raises(RuntimeError,match='Publication changed'):run_query('publication',{},None,'other')


def test_code_fingerprint_mismatch_never_runs_sql():
    from analytics.access import run_query
    @contextlib.contextmanager
    def admitted(*args):yield
    @contextlib.contextmanager
    def window():yield {"batch":"batch","context":{"code_fingerprint":"published"}}
    with patch('analytics.access.admission',admitted),patch('gold.consumer.read_window',window), \
         patch('gold.admission.code_fingerprint',return_value='tampered'),patch('analytics.access.execute') as sql:
        with pytest.raises(RuntimeError,match='fingerprint'):run_query('fact_overview',{'fact':'fact_nso_observation'},None)
        sql.assert_not_called()
