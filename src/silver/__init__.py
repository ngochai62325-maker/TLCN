"""Silver Layer package for Vietnam Rice Lakehouse with backward-compatible module aliases."""
import sys

from . import engine, framework  # noqa: E402

for _alias, _target in {
    "audit": framework,
    "local_profile": framework,
    "orchestration": framework,
    "contract": engine,
    "contract.loader": engine,
    "core": engine,
    "core.spark_session": engine,
    "core.base_transformer": engine,
    "engine.merge": engine,
    "engine.quality": engine,
    "engine.quarantine": engine,
}.items():
    setattr(sys.modules[__name__], _alias.split(".")[0], _target)
    sys.modules[f"{__name__}.{_alias}"] = _target
    sys.modules[f"src.{__name__}.{_alias}"] = _target

