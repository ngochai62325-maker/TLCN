import os
import yaml
from typing import Dict, List, Optional
import re

from ingestion.core.enums import SourceType, LoadStrategy, ArtifactFormat
from ingestion.core.config import SourceConfig, RetryConfig, ReadinessConfig
from ingestion.utils.error_classifier import PermanentError

class SourceRegistry:
    def __init__(self, registry_path: Optional[str] = None):
        if not registry_path:
            self.registry_path = os.path.join(os.path.dirname(__file__), "source_registry.yaml")
        else:
            self.registry_path = registry_path
        self._sources: Dict[str, SourceConfig] = {}

    def load(self) -> None:
        if not os.path.exists(self.registry_path):
            raise PermanentError(f"Registry file not found at {self.registry_path}")
            
        with open(self.registry_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            
        if not data or "sources" not in data:
            return
            
        for source_id, raw_config in data["sources"].items():
            self._sources[source_id] = self._parse_source_config(source_id, raw_config)

    def _substitute_env_vars(self, value: str) -> str:
        if not isinstance(value, str):
            return value
        pattern = re.compile(r'\$\{([^}^{]+)\}')
        def replace(match):
            env_var = match.group(1)
            return os.environ.get(env_var, f"${{{env_var}}}")
        return pattern.sub(replace, value)

    def _parse_source_config(self, source_id: str, raw_config: dict) -> SourceConfig:
        endpoint = raw_config.get("endpoint")
        if endpoint:
            endpoint = self._substitute_env_vars(endpoint)
            
        retry_dict = raw_config.get("retry", {})
        retry_config = RetryConfig(**retry_dict) if retry_dict else RetryConfig()
        
        readiness_dict = raw_config.get("readiness", {})
        readiness_config = ReadinessConfig(**readiness_dict) if readiness_dict else None
        
        return SourceConfig(
            source_id=source_id,
            provider=raw_config.get("provider", ""),
            dataset=raw_config.get("dataset", ""),
            source_type=SourceType[raw_config.get("source_type", "LOCAL_FILE")],
            load_strategy=LoadStrategy[raw_config.get("load_strategy", "FULL")],
            format=ArtifactFormat[raw_config.get("format", "CSV")],
            endpoint=endpoint,
            local_path=raw_config.get("local_path"),
            chunk_size=raw_config.get("chunk_size"),
            encoding=raw_config.get("encoding"),
            readiness=readiness_config,
            retry=retry_config,
            frequency=raw_config.get("frequency"),
            watermark_column=raw_config.get("watermark_column"),
            business_key=raw_config.get("business_key"),
            partition_column=raw_config.get("partition_column"),
            expected_columns=raw_config.get("expected_columns"),
            adapter_class=raw_config.get("adapter_class", ""),
            extra=raw_config.get("extra", {})
        )

    def get_source(self, source_id: str) -> SourceConfig:
        if source_id not in self._sources:
            raise PermanentError(f"Source ID '{source_id}' not found in registry.")
        return self._sources[source_id]

    def list_sources(self) -> List[str]:
        return list(self._sources.keys())

    def get_all_sources(self) -> Dict[str, SourceConfig]:
        return self._sources.copy()
