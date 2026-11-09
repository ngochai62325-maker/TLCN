import copy
from unittest.mock import patch

import pytest

from gold.keys import fingerprint, key
from gold.spec import load_spec
from gold.trade_validation import (bucket, partitions, partition_sql, predicate, check_result,
                                  validate_trade, valid_checkpoint, verify_trade_summary, resource_guard, settle_heap)


class Objects:
    def __init__(self):
        self.data = {}

    def get(self, k):
        return copy.deepcopy(self.data.get(k))

    def put(self, k, v, immutable=False):
        if immutable and k in self.data and self.data[k] != v:
            raise ValueError("Immutable conflict")
        self.data[k] = copy.deepcopy(v)


class DQ:
    def record(self, rule, scope, description, expected, actual, ok=True):
        assert ok and expected == actual


def outputs():
    return {n:{"uuid":n,"snapshot_id":"1","rows":3,"metadata_location":"s3://"+n,
               "data_files":["s3://"+n+".parquet"]} for n in load_spec()}


def result(count=3, groups=2):
    return [["actual",groups,groups,0,0],["expected",count,groups,0,0],["forward",0,0,0,0],["reverse",0,0,0,0]]


def response(rows):
    return {"rows":rows,"query_id":"fixture-query","stats":{},"client_elapsed_seconds":.01}


def test_deterministic_bucket_exhaustive_disjoint_and_group_containment():
    # Exhaust every possible first byte, plus many groups and paired metrics.
    for prefix in range(256):
        reporter = f"{prefix:02x}"+"a"*62
        assert bucket(reporter) == prefix % 4
        for partner in ("p1","p2"):
            for product in ("rice","paddy"):
                positions = {bucket(reporter) for metric in ("5610","5622","5910","5922")}
                assert positions == {prefix % 4}
    assert {bucket(key("reporter",[i])) for i in range(100)} == set(range(4))


@pytest.mark.parametrize("bad", [None,"g1","A"*64,"0"*63,42])
def test_invalid_reporter_rejected(bad):
    with pytest.raises(ValueError): bucket(bad)


@pytest.mark.parametrize("rows,total", [([[2017,0,3,0],[2017,0,3,0]],6), ([[2017,0,3,0]],4),
    ([[2017,4,3,0]],3), ([[2017,0,3,1]],3), ([[None,0,3,0]],3)])
def test_domain_rejects_duplicate_missing_invalid(rows,total):
    with pytest.raises(ValueError): partitions(rows,total)


def test_empty_and_skewed_domain():
    assert partitions([],0) == []
    actual = partitions([[2018,3,1,0],[2017,0,100000,0],[2017,2,2,0]],100003)
    assert [(p['year'],p['bucket']) for p in actual] == [(2017,0),(2017,2),(2018,3)]


def test_sql_preserves_full_grain_metric_contributors_null_and_both_multisets():
    sql = partition_sql(outputs(),2017,2)
    assert sql.count('EXCEPT ALL') == 2
    assert 'LIMIT' not in sql and 'approx_distinct' not in sql
    assert 'u.metric,u.observation_key,u.value' in sql
    assert 'partner_geography_key' in sql and "u.presence=1" in sql
    assert 'present_5610=0 AND import_quantity_t IS NOT NULL' in sql
    assert 'IS DISTINCT FROM' in sql
    assert predicate(2017,2,'q') in sql and predicate(2017,2,'f') in sql


@pytest.mark.parametrize("index,column", [(0,1),(0,2),(0,3),(0,4),(1,1),(1,2),(2,1),(3,1)])
def test_exact_results_reject_grain_presence_ratio_missing_extra(index,column):
    rows = result(); rows[index][column] += 1
    with pytest.raises(ValueError): check_result(rows,{'input_rows':3})


def test_empty_partition_and_many_observations_one_group():
    assert check_result(result(0,0),{'input_rows':0})['actual']['rows'] == 0
    assert check_result(result(400,1),{'input_rows':400})['expected']['rows'] == 400


def run(store, replies, **kwargs):
    with patch('gold.trade_validation.resource_guard', return_value={'used':1,'max':100}), \
         patch('gold.trade_validation.heap_state', return_value={'used':2,'max':100}), \
         patch('gold.views.trino_result', side_effect=replies):
        return validate_trade(outputs(),DQ(),store,{'code_fingerprint':'reviewed-code'},**kwargs)


def test_checkpoint_resume_is_exact_deterministic_and_no_query_reexecution():
    store=Objects(); domain=response([[2017,0,3,0]])
    first=run(store,[domain,response(result())])
    verify_trade_summary(first,3)
    second=run(store,[domain])
    assert second == first  # immutable publication must survive formal replay
    assert store.get(first['progress_key'])['reused_checkpoints'] == 1


def test_failure_keeps_progress_incomplete_and_only_completed_partition_accepted():
    store=Objects(); domain=response([[2017,0,1,0],[2017,1,2,0]])
    with pytest.raises(TimeoutError): run(store,[domain,response(result(1,1)),TimeoutError('query timeout')])
    progress=next(v for k,v in store.data.items() if k.endswith('progress.json'))
    assert progress['status']=='INCOMPLETE' and len(progress['accepted'])==1
    complete=run(store,[domain,response(result(2,1))])
    verify_trade_summary(complete,3)


def test_partial_scope_never_accepted_as_full():
    store=Objects(); domain=response([[2017,0,1,0],[2018,1,2,0]])
    partial=run(store,[domain,response(result(1,1))],year=2017)
    assert partial['status']=='INCOMPLETE'
    with pytest.raises(ValueError): verify_trade_summary(partial,3)


@pytest.mark.parametrize('mutation',['result','scope','status','seal'])
def test_tampered_or_incompatible_checkpoint_rejected(mutation):
    store=Objects(); run(store,[response([[2017,0,3,0]]),response(result())])
    saved=next(v for k,v in store.data.items() if k.endswith('2017_0.json'))
    original=copy.deepcopy(saved)
    if mutation=='result': saved['result']['forward']['rows']=1
    else: saved[mutation]='incorrect'
    with pytest.raises(ValueError): valid_checkpoint(saved,original['scope'],original['partition'])


def test_changed_definition_algorithm_or_code_cannot_reuse_checkpoint():
    store=Objects(); first=run(store,[response([[2017,0,3,0]]),response(result())])
    with patch('gold.trade_validation.VERSION','different-algorithm'):
        second=run(store,[response([[2017,0,3,0]]),response(result())])
    assert first['scope'] != second['scope']


def test_changed_code_context_requires_fresh_query_and_new_checkpoint_scope():
    store=Objects(); domain=response([[2017,0,3,0]])
    first=run(store,[domain,response(result())])
    with patch('gold.trade_validation.resource_guard',return_value={}), \
         patch('gold.trade_validation.heap_state',return_value={'used':2,'max':100}), \
         patch('gold.views.trino_result',side_effect=[domain,response(result())]) as execute:
        second=validate_trade(outputs(),DQ(),store,{'code_fingerprint':'new-reviewed-code'})
    assert execute.call_count==2 and first['scope']!=second['scope']
    assert store.get(second['progress_key'])['reused_checkpoints']==0


def test_heap_anomaly_fails_closed_without_accept_checkpoint():
    store=Objects()
    with patch('gold.trade_validation.resource_guard',return_value={}), \
         patch('gold.trade_validation.settle_heap',side_effect=RuntimeError('heap anomaly')), \
         patch('gold.views.trino_result',side_effect=[response([[2017,0,3,0]]),response(result())]):
        with pytest.raises(RuntimeError,match='heap anomaly'):
            validate_trade(outputs(),DQ(),store,{'code_fingerprint':'code'})
    assert not any(k.endswith('2017_0.json') for k in store.data)
    progress=next(v for k,v in store.data.items() if k.endswith('progress.json'))
    assert progress['status']=='INCOMPLETE' and progress['last_attempt']['query_id']=='fixture-query'


def test_transient_heap_waits_then_accepts_without_query_retry():
    with patch('gold.trade_validation.heap_state',side_effect=[{'used':95,'max':100},{'used':65,'max':100}]) as read, \
         patch('gold.trade_validation.time.sleep') as wait:
        state=settle_heap()
    assert state['samples']==2 and state['sampled_peak_bytes']==95 and state['used']==65
    assert wait.call_count==1
    with patch('gold.trade_validation.heap_state',return_value={'used':95,'max':100}):
        with pytest.raises(RuntimeError,match='did not settle'): settle_heap(max_wait=0)


def test_client_row_and_timeout_budgets():
    from gold.views import trino_result
    with patch('gold.views.trino_json',return_value={'id':'id','data':[[1],[2]]}):
        with pytest.raises(RuntimeError,match='budget'): trino_result('SELECT 1',max_rows=1)
    with patch('gold.views.trino_json',return_value={'id':'id'}), patch('gold.views.time.monotonic',side_effect=[0,181]):
        with pytest.raises(RuntimeError,match='budget'): trino_result('SELECT 1')


def test_consumer_trade_uses_verified_bucket_before_limit():
    from contextlib import contextmanager
    from gold.consumer import query
    from gold.views import expected_schema, VIEW_NAMES
    from test_semantic_delivery import published
    manifest=published()
    @contextmanager
    def window(): yield manifest
    with patch('gold.consumer.read_window',window), patch('gold.consumer.trino_result',return_value={'columns':expected_schema(VIEW_NAMES[3]),'rows':[[1]]}) as execute:
        result=query(VIEW_NAMES[3],2024)
    assert 'from_base(substr(q.reporter_geography_key,1,2)' in execute.call_args[0][0]
    assert result['sample_partitions']==[{'year':2024,'bucket':0}]


def test_resource_guard_active_query_and_timeout_fail_closed():
    with patch('gold.trade_validation.heap_state',return_value={'used':1,'max':100}), \
         patch('gold.views.trino_json',return_value=[{'state':'RUNNING'}]):
        with pytest.raises(RuntimeError,match='sequential idle'): resource_guard()
    with patch('gold.trade_validation.heap_state',return_value={'used':99,'max':100}), \
         patch('gold.views.trino_json',return_value=[]):
        with pytest.raises(RuntimeError,match='admission blocked'): resource_guard(max_wait=0)


def test_small_subject_validation_is_full_scope_and_preserves_grain_metric():
    from gold.views import validate_semantic_queries,expected_schema,VIEW_NAMES
    def answer(sql):
        if 'DISTINCT year_key' in sql: return [[2017],[2024]]
        return [[0]] if 'EXCEPT ALL' in sql or 'AND (' in sql or 'IS DISTINCT FROM' in sql else [[3]]
    def schema(sql):
        name=next(n for n in VIEW_NAMES if ('fact_nso_observation' if n==VIEW_NAMES[0] else
              'fact_fao_production_observation' if n==VIEW_NAMES[1] else
              'fact_fao_sua_observation' if n==VIEW_NAMES[2] else 'fact_fao_trade_observation') in sql)
        return {'columns':expected_schema(name)}
    with patch('gold.trade_validation.resource_guard'),patch('gold.trade_validation.validate_trade',return_value={'status':'ACCEPT'}), \
         patch('gold.views.trino_result',side_effect=schema),patch('gold.views.trino',side_effect=answer) as execute:
        records=validate_semantic_queries(outputs(),DQ(),Objects(),{'code_fingerprint':'code'})
    for n in VIEW_NAMES[:3]:
        assert records[n]['validation']['coverage']=='FULL_SOURCE_SCOPE'
        assert records[n]['validation']['partition_years']==[2017,2024]
    exact=[c.args[0] for c in execute.call_args_list if 'EXCEPT ALL' in c.args[0]]
    assert len(exact)==4 and all('source_metric_id' in sql and 'geography_key' in sql for sql in exact)
