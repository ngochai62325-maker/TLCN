"""Source Registry loader, validator, and repository.

Loads source configurations from ``source_registry.yaml``, validates contracts,
enforces non-fake incremental rules, and supplies configurations to adapters,
loaders, and the ingestion engine.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional
import yaml

from ingestion.core.config import ReadinessConfig, RetryConfig, SourceConfig
from ingestion.core.enums import ArtifactFormat, LoadStrategy, SourceType
from ingestion.utils.error_classifier import PermanentError


class RegistryValidationError(PermanentError):
    """Raised when a source configuration fails registry contract validation."""
    pass


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML Loader that enforces unique keys at all dictionary levels."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> Dict[Any, Any]:
        mapping: Dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise RegistryValidationError(
                    f"Duplicate source_id or configuration key detected in YAML: '{key}'"
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


class SourceRegistry:
    """Repository of declared data sources and their ingestion specifications."""

    def __init__(self, registry_path: Optional[str] = None) -> None:
        if not registry_path:
            self.registry_path = os.path.join(os.path.dirname(__file__), "source_registry.yaml")
        else:
            self.registry_path = registry_path
        self._sources: Dict[str, SourceConfig] = {}
        self._aliases: Dict[str, str] = {}

    def load(self, validate: bool = True, validate_filesystem: bool = False) -> None:
        """Load sources from YAML file, register aliases, and optionally validate."""
        if not os.path.exists(self.registry_path):
            raise PermanentError(f"Registry file not found at {self.registry_path}")

        with open(self.registry_path, "r", encoding="utf-8") as f:
            data = yaml.load(f, Loader=UniqueKeyLoader)

        if not data or "sources" not in data:
            return

        raw_sources = data["sources"]
        if not isinstance(raw_sources, dict):
            raise RegistryValidationError("'sources' root element must be a dictionary of sources.")

        self._sources.clear()
        self._aliases.clear()

        for source_id, raw_config in raw_sources.items():
            config = self._parse_source_config(source_id, raw_config)
            self._sources[source_id] = config
            for alias in config.aliases:
                self._aliases[alias] = source_id

        if validate:
            self.validate(validate_filesystem=validate_filesystem)

    def _substitute_env_vars(self, value: str) -> str:
        """Replace ${VAR} with environment variable value."""
        if not isinstance(value, str):
            return value
        pattern = re.compile(r"\$\{([^}^{]+)\}")

        def replace(match: re.Match[str]) -> str:
            env_var = match.group(1)
            return os.environ.get(env_var, f"${{{env_var}}}")

        return pattern.sub(replace, value)

    def _parse_source_config(self, source_id: str, raw_config: dict) -> SourceConfig:
        """Parse and construct a SourceConfig instance, checking validation rules."""
        if not isinstance(source_id, str) or not source_id.strip():
            raise RegistryValidationError("source_id must be a non-empty string.")

        if not re.match(r"^[a-zA-Z0-9_-]+$", source_id):
            raise RegistryValidationError(
                f"Invalid source_id format '{source_id}'. Only alphanumeric, underscores, and hyphens allowed."
            )

        if not isinstance(raw_config, dict):
            raise RegistryValidationError(f"Configuration for source '{source_id}' must be a dictionary.")

        # Required fields
        provider = raw_config.get("provider")
        if not provider or not str(provider).strip():
            raise RegistryValidationError(f"Source '{source_id}' is missing required field 'provider'.")

        dataset = raw_config.get("dataset")
        if not dataset or not str(dataset).strip():
            raise RegistryValidationError(f"Source '{source_id}' is missing required field 'dataset'.")

        # SourceType enum validation
        raw_source_type = raw_config.get("source_type")
        if not raw_source_type:
            raise RegistryValidationError(f"Source '{source_id}' is missing required field 'source_type'.")
        try:
            source_type = SourceType[raw_source_type]
        except KeyError:
            valid_types = [t.name for t in SourceType]
            raise RegistryValidationError(
                f"Source '{source_id}' has invalid source_type '{raw_source_type}'. Expected one of {valid_types}."
            )

        # LoadStrategy enum validation
        raw_load_strategy = raw_config.get("load_strategy")
        if not raw_load_strategy:
            raise RegistryValidationError(f"Source '{source_id}' is missing required field 'load_strategy'.")
        try:
            load_strategy = LoadStrategy[raw_load_strategy]
        except KeyError:
            valid_strategies = [s.name for s in LoadStrategy]
            raise RegistryValidationError(
                f"Source '{source_id}' has invalid load_strategy '{raw_load_strategy}'. Expected one of {valid_strategies}."
            )

        # Format enum validation
        raw_format = raw_config.get("format")
        if not raw_format:
            raise RegistryValidationError(f"Source '{source_id}' is missing required field 'format'.")
        try:
            artifact_format = ArtifactFormat[raw_format]
        except KeyError:
            valid_formats = [f.name for f in ArtifactFormat]
            raise RegistryValidationError(
                f"Source '{source_id}' has invalid format '{raw_format}'. Expected one of {valid_formats}."
            )

        # Non-fake incremental validation
        watermark_col = raw_config.get("watermark_column")
        if load_strategy == LoadStrategy.INCREMENTAL and not watermark_col:
            raise RegistryValidationError(
                f"Source '{source_id}' is configured as INCREMENTAL but missing 'watermark_column'. "
                "Fake incremental is strictly disallowed."
            )

        # Endpoint / Source URL
        endpoint = raw_config.get("endpoint") or raw_config.get("source_url")
        if endpoint:
            endpoint = self._substitute_env_vars(endpoint)

        local_path = raw_config.get("local_path")
        local_fallback = raw_config.get("local_fallback")

        enabled = bool(raw_config.get("enabled", True))

        # Location requirement for enabled sources
        if enabled:
            if source_type == SourceType.LOCAL_FILE:
                if not local_path and not local_fallback:
                    raise RegistryValidationError(
                        f"Enabled LOCAL_FILE source '{source_id}' must specify 'local_path' or 'local_fallback'."
                    )
            elif source_type in (SourceType.HTTP_BULK_ZIP, SourceType.HTTP_FILE, SourceType.API):
                if not endpoint and not local_fallback:
                    raise RegistryValidationError(
                        f"Enabled remote source '{source_id}' must specify 'endpoint'/'source_url' or 'local_fallback'."
                    )

        # Schema & Columns validation
        expected_columns = raw_config.get("expected_columns")
        if expected_columns is not None:
            if not isinstance(expected_columns, (list, tuple)) or not all(isinstance(c, str) for c in expected_columns):
                raise RegistryValidationError(
                    f"Source '{source_id}' has invalid 'expected_columns'. Must be a list of strings."
                )

        expected_schema = raw_config.get("expected_schema")
        if expected_schema is not None and not isinstance(expected_schema, dict):
            raise RegistryValidationError(
                f"Source '{source_id}' has invalid 'expected_schema'. Must be a dictionary."
            )

        schema_status = raw_config.get("schema_status")
        if schema_status:
            if schema_status not in ("defined", "pending_adapter_profiling"):
                raise RegistryValidationError(
                    f"Source '{source_id}' has invalid schema_status '{schema_status}'. "
                    "Must be 'defined' or 'pending_adapter_profiling'."
                )
        else:
            schema_status = "defined" if expected_columns else "pending_adapter_profiling"

        # Policies
        retry_dict = raw_config.get("retry", {})
        retry_config = RetryConfig(**retry_dict) if retry_dict else RetryConfig()

        readiness_dict = raw_config.get("readiness", {})
        readiness_config = ReadinessConfig(**readiness_dict) if readiness_dict else None

        # Aliases
        raw_aliases = raw_config.get("aliases", [])
        if isinstance(raw_aliases, str):
            aliases = [raw_aliases]
        elif isinstance(raw_aliases, (list, tuple)):
            aliases = list(raw_aliases)
        else:
            aliases = []

        source_name = raw_config.get("source_name") or f"{provider} - {dataset}"

        return SourceConfig(
            source_id=source_id,
            provider=str(provider),
            dataset=str(dataset),
            source_type=source_type,
            load_strategy=load_strategy,
            format=artifact_format,
            endpoint=endpoint,
            local_path=local_path,
            local_fallback=local_fallback,
            chunk_size=raw_config.get("chunk_size", 50_000),
            encoding=raw_config.get("encoding", "utf-8"),
            readiness=readiness_config or ReadinessConfig(),
            retry=retry_config,
            frequency=raw_config.get("frequency", "annual"),
            watermark_column=watermark_col,
            business_key=raw_config.get("business_key"),
            partition_column=raw_config.get("partition_column"),
            expected_columns=list(expected_columns) if expected_columns else None,
            adapter_class=raw_config.get("adapter_class", ""),
            extra=raw_config.get("extra", {}),
            source_name=source_name,
            enabled=enabled,
            schema_status=schema_status,
            expected_schema=expected_schema,
            aliases=aliases,
        )

    def validate(self, validate_filesystem: bool = False) -> None:
        """Validate all registered sources against configuration rules."""
        if not self._sources:
            raise RegistryValidationError("SourceRegistry has no registered sources.")

        for source_id, config in self._sources.items():
            if not config.source_id or not config.provider or not config.dataset:
                raise RegistryValidationError(f"Source '{source_id}' failed basic completeness validation.")

            if config.enabled:
                if config.source_type == SourceType.LOCAL_FILE:
                    active_path = config.local_path or config.local_fallback
                    if not active_path:
                        raise RegistryValidationError(
                            f"Enabled source '{source_id}' is missing a local file path."
                        )
                    if validate_filesystem and not os.path.exists(active_path):
                        raise RegistryValidationError(
                            f"Local path '{active_path}' for source '{source_id}' does not exist on disk."
                        )
                elif config.source_type in (SourceType.HTTP_BULK_ZIP, SourceType.HTTP_FILE, SourceType.API):
                    if not config.endpoint and not config.local_fallback:
                        raise RegistryValidationError(
                            f"Enabled remote source '{source_id}' has neither endpoint nor local fallback."
                        )

    def register(self, config: SourceConfig) -> None:
        """Programmatically register a source configuration with validation."""
        if config.source_id in self._sources:
            raise RegistryValidationError(f"Source '{config.source_id}' is already registered.")
        self._sources[config.source_id] = config
        for alias in config.aliases:
            self._aliases[alias] = config.source_id

    def get_source(self, source_id: str) -> SourceConfig:
        """Retrieve source config by primary source_id or alias."""
        if source_id in self._sources:
            return self._sources[source_id]
        if source_id in self._aliases:
            return self._sources[self._aliases[source_id]]
        raise PermanentError(f"Source ID '{source_id}' not found in registry.")

    def has_source(self, source_id: str, include_aliases: bool = True) -> bool:
        """Check whether a source ID (or alias) is registered."""
        if source_id in self._sources:
            return True
        if include_aliases and source_id in self._aliases:
            return True
        return False

    def list_sources(self, include_aliases: bool = False) -> List[str]:
        """Return list of registered source identifiers."""
        if include_aliases:
            return list(self._sources.keys()) + list(self._aliases.keys())
        return list(self._sources.keys())

    def get_all_sources(self, enabled_only: bool = False) -> Dict[str, SourceConfig]:
        """Return dictionary copy of sources."""
        if enabled_only:
            return {k: v for k, v in self._sources.items() if v.enabled}
        return self._sources.copy()
