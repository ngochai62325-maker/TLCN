# Evidence index

Current demo runtime: [Phase B](demo_phase_b_20261017_131025/README.md),
**LOCAL_DEMO_RUNTIME_READY_PENDING_OWNER_PIPELINE**. Fresh `tlcn-local` has
smoke PASS, zero DagRuns and no loaded lakehouse tables at verification.
Older runtime data was retired/deleted with owner approval; the data acceptance
and physical NOOP proofs below remain historical Git evidence.

Historical data acceptance: **LOCAL_NINE_SILVER_COMPLETE_CLEANROOM_NOOP_PASS** for the pinned
local bundle. Final regression206 PASS/1 opt-in SKIP, cleanup and Git secret/style review PASS.
NSO production remains DRAFT. No Gold, production scheduling or consumer cutover.

| Claim | Durable evidence |
|---|---|
| Eight-source release closed manual-only | [Release closure](release_20261013/closure.json) |
| NSO13 CSV REST native/Trino, units and replay | [Phase1](nso_v2/phase1_rest_20261016/README.md) |
| Earlier nine-source Airflow/Trino acceptance | [Phase2 final readback](nso_v2/phase2_trino_adjustment_20261016_152237/README.md) |
| Original clean-room run, startup history and active Compose overlay | [Initial project](nso_v2/phase3_cleanroom_20261016_154548/) |
| Initial NSO recovery and seven native quarantine tests | [Targeted NSO evidence](nso_v2/phase3_nso_targeted_20261017_015603/README.md) |
| Original DagRuns SUCCESS and initial nine-source native/Trino | [Initial acceptance](nso_v2/phase3_final_20261017_024536/README.md) |
| Interrupted full replay, targeted recovery, incremental and physical NOOP | [Current completion](noop_restart_20261017_063554/README.md) |
| Baseline all19 active-file SHA256 and dead-letter audit | [Before replay](nso_v2/phase3_final_20261017_024536/physical-before-replay.json) |
| Full replay physical comparison | [Full proof](noop_restart_20261017_063554/physical-noop-after-full.json) |
| Incremental physical comparison | [Incremental proof](noop_restart_20261017_063554/physical-noop-after-incremental.json) |
| Old live documentation, preserved byte-for-byte | [Archive manifest](../archive/manifest.json), [ZIP](../archive/before_local_consolidation.zip) |

Historical failure/progress records remain available to defend recovery claims;
they are not the current navigation or a claim of uninterrupted success. No
accepted report is rewritten to convert an old failure into PASS. The active
clean-room Compose overlay stays at its original evidence path.

The current report/manifest records source/run/snapshot IDs, real counts, file
hashes, test outcomes, known startup/interruption histories and final paused state.
The34 historical unreferenced NSO files are retained; no snapshot rollback,
orphan adoption or object deletion was performed.
