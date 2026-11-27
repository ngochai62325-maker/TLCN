"""Read-only, publication-bound EDA with backward-compatible module aliases."""
import glob
import os
import sys

_spark_home = os.environ.get("SPARK_HOME")
if _spark_home:
    _spark_py = os.path.join(_spark_home, "python")
    if os.path.isdir(_spark_py):
        for _path in (_spark_py, *sorted(glob.glob(os.path.join(_spark_py, "lib", "py4j-*.zip")))):
            if _path not in sys.path:
                sys.path.append(_path)

VERSION = "eda-20261027.1"

from . import queries, client, powerbi  # noqa: E402

for _alias, _target in {
    "access": client,
    "transport": client,
    "methods": client,
    "notebook": client,
    "powerbi_fence": powerbi,
}.items():
    setattr(sys.modules[__name__], _alias, _target)
    sys.modules[f"{__name__}.{_alias}"] = _target
    sys.modules[f"src.{__name__}.{_alias}"] = _target

