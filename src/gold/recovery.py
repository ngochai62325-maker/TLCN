"""Explicit failed-semantic-stage recovery; never mutates data/catalog snapshots."""
import copy

from gold import MODEL_VERSION, OWNER
from gold.keys import fingerprint
from gold.spec import load_spec


def recovered_state(previous, context, owner_run_id, actual_outputs):
    if (previous.get("status") != "FAILED_NOT_PUBLISHED" or previous.get("owner") != OWNER
            or previous.get("model") != MODEL_VERSION or previous.get("error", {}).get("action") != "validate"
            or set(previous.get("outputs", {})) != set(load_spec())
            or {k: v for k, v in previous["context"].items() if k != "code_fingerprint"} != {k: v for k, v in context.items() if k != "code_fingerprint"}):
        raise ValueError("Recovery requires failed validation, full owned outputs and identical pinned inputs/references/model/scope")
    for name, output in previous["outputs"].items():
        actual = actual_outputs[name]
        for field in ("uuid", "snapshot_id", "metadata_location", "rows", "data_files"):
            if actual[field] != output[field]:
                raise ValueError(f"Unknown/drifted commit outcome: {name}/{field}")
    result = copy.deepcopy(previous)
    result.update(batch=fingerprint(context), context=context, owner_run_id=owner_run_id, status="RECOVERY_ADMITTED", dq=[])
    result.pop("error", None)
    result.pop("semantic_queries", None)
    result.pop("reconciliation", None)
    result["recovery"] = {"from_batch": previous["batch"], "from_run": previous["owner_run_id"], "reason": previous["error"], "output_snapshot_set_verified": True, "data_writes": False}
    return result
