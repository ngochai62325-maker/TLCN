"""Source-scoped local Gold model with backward-compatible module aliases."""
import importlib
import importlib.abc
import importlib.util
import sys

MODEL_VERSION = "gold-20261025.1"
INPUT_NAMESPACE = "iceberg.silver_local"
OUTPUT_NAMESPACE = "iceberg.gold_local"
OWNER = "tlcn-local"
ACTIONS = ("preflight", "dimensions", "fact", "validate", "publish")
BUILD_ORDER = ("worldbank_pinksheet", "faostat_production", "faostat_supply_utilization", "faostat_trade", "usda_psd", "nso", "thitruongnongsan", "faostat_monthly_price", "usda_rice_yearbook")

from . import spec, pipeline  # noqa: E402

for _alias, _target in {
    "rules": spec,
    "catalog": spec,
    "admission": pipeline,
    "preflight": pipeline,
    "publication": pipeline,
    "recovery": pipeline,
    "consumer": pipeline,
    "orchestration": pipeline,
    "api": pipeline,
}.items():
    setattr(sys.modules[__name__], _alias, _target)
    sys.modules[f"{__name__}.{_alias}"] = _target
    sys.modules[f"src.{__name__}.{_alias}"] = _target


_LAZY_ALIASES = {
    f"{__name__}.trade_validation": f"{__name__}.quality",
    f"src.{__name__}.trade_validation": f"{__name__}.quality",
}


class _LazyAliasLoader(importlib.abc.Loader):
    def __init__(self, target_name):
        self.target_name = target_name

    def create_module(self, spec_obj):
        mod = importlib.import_module(self.target_name)
        sys.modules[spec_obj.name] = mod
        return mod

    def exec_module(self, module):
        pass


class _LazyAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        target_name = _LAZY_ALIASES.get(fullname)
        if target_name is None:
            return None
        return importlib.util.spec_from_loader(fullname, _LazyAliasLoader(target_name))


if not any(isinstance(f, _LazyAliasFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _LazyAliasFinder())


def __getattr__(name):
    if name in ("quality", "trade_validation"):
        mod = importlib.import_module(f"{__name__}.quality")
        setattr(sys.modules[__name__], name, mod)
        sys.modules[f"{__name__}.trade_validation"] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

