import json
from pathlib import Path


def test_six_owned_notebooks_have_independent_cells_and_no_sample_totals():
    paths=sorted(Path('notebooks/analytics').glob('*.ipynb'))
    assert len(paths)==6
    for path in paths:
        nb=json.loads(path.read_text(encoding='utf-8'))
        code=[''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code']
        markdown='\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type']=='markdown')
        assert len(code)>=8 and max(len(c.splitlines()) for c in code)<85 and '## Key Findings' in markdown
        assert 'EDAClient()' in code[0] and "client.query('publication')" in code[0]
        assert all('query_gold.py' not in c and 'spark.' not in c for c in code)
        assert len({c['id'] for c in nb['cells']})==len(nb['cells'])
