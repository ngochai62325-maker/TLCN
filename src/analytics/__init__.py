"""Read-only, publication-bound EDA with backward-compatible module aliases."""
import sys

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

