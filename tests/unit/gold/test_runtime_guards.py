import io
import json
import unittest
from unittest.mock import patch
import urllib.error

from gold.views import require_semantic_definitions, VIEW_NAMES
from gold.spec import load_spec


class Guards(unittest.TestCase):
    def test_semantic_definitions_are_exact_allowlist(self):
        self.assertEqual(set(require_semantic_definitions()), set(VIEW_NAMES))

    def test_dag_is_manual_serial_and_no_upstream_trigger(self):
        import ast
        from gold.spec import ROOT
        text = (ROOT / "dags/dag_gold_pipeline.py").read_text(encoding="utf-8")
        tree = ast.parse(text)
        dag = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "DAG")
        args = {k.arg: ast.literal_eval(k.value) for k in dag.keywords if k.arg in {"schedule", "catchup", "max_active_runs", "max_active_tasks"}}
        self.assertEqual(args, {"schedule": None, "catchup": False, "max_active_runs": 1, "max_active_tasks": 1})
        self.assertNotIn("TriggerDagRunOperator", text)

    def test_semantic_readiness_never_calls_native_view_route(self):
        with patch("gold.views.urllib.request.urlopen", side_effect=AssertionError("no REST view calls")):
            self.assertEqual(len(require_semantic_definitions()), 4)

    def test_unenforced_iceberg_constraints_have_explicit_runtime_checks(self):
        specs = load_spec()
        self.assertEqual(sum(bool(c.reference) for t in specs.values() for c in t.columns), 40)


if __name__ == "__main__":
    unittest.main()
