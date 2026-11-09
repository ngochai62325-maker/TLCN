import copy

import pytest

from gold import OWNER, MODEL_VERSION
from gold.keys import fingerprint
from gold.recovery import recovered_state
from gold.spec import load_spec


def evidence():
    context = {"model": MODEL_VERSION, "inputs": {"source": {"uuid": "silver", "snapshot_id": "123"}}, "code_fingerprint": "old", "reference_fingerprints": {"schema": "same"}}
    outputs = {n: {"uuid": "gold-" + n, "snapshot_id": "456", "metadata_location": "metadata-" + n, "rows": 1, "data_files": ["file-" + n]} for n in load_spec()}
    previous = {"owner": OWNER, "model": MODEL_VERSION, "batch": fingerprint(context), "owner_run_id": "failed-run", "context": context, "outputs": outputs, "status": "FAILED_NOT_PUBLISHED", "error": {"action": "validate"}, "dq": [{"status": "ACCEPT"}], "completed_sources": []}
    new = {**context, "code_fingerprint": "fixed"}
    return previous, new, copy.deepcopy(outputs)


def test_recovery_preserves_all_data_snapshots_and_failed_evidence():
    previous, new, actual = evidence()
    saved = copy.deepcopy(previous)
    result = recovered_state(previous, new, "new-run", actual)
    assert previous == saved
    assert result["outputs"] == actual
    assert result["batch"] == fingerprint(new)
    assert result["recovery"]["data_writes"] is False
    assert result["status"] == "RECOVERY_ADMITTED"


@pytest.mark.parametrize("change", ["source", "reference", "uuid", "snapshot_id", "metadata_location", "data_files", "rows", "stage"])
def test_recovery_refuses_drift_and_unknown_commits(change):
    previous, new, actual = evidence()
    if change == "source":
        new = {**new, "inputs": {"source": {"snapshot_id": "999"}}}
    elif change == "reference":
        new = {**new, "reference_fingerprints": {"schema": "changed"}}
    elif change == "stage":
        previous["error"]["action"] = "fact"
    else:
        actual["dim_year"][change] = "drifted"
    with pytest.raises(ValueError):
        recovered_state(previous, new, "new-run", actual)
