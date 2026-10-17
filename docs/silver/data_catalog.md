# Nine-source Silver data catalog

Initial clean-room native/Trino acceptance passed; after the Docker shutdown,
read-only Parquet scans and SHA-256 verified every active file of all19 tables
unchanged. **Full and incremental physical NOOP PASS; see actual run/mode proofs below.**
See [current evidence](evidence/index.md); this catalog does not promote NSO DRAFT.

All targets below have prefix `iceberg.silver_local.`. Schemas and business keys
are authoritative in [contracts/silver](../../contracts/silver/).

| Bronze source / Silver dataset | Target | Bronze rows | Silver rows | Units |
|---|---|---:|---:|---|
| `faostat_production` / `faostat_production` | `faostat_production` | 22738 | 22738 | hectare, kg/hectare, tonne |
| `faostat_monthly_price` / `faostat_monthly_price` | `faostat_monthly_price` | 15334 | 15334 | None, LCU, SLC, USD |
| `faostat_supply_utilization` / `faostat_supply_utilization` | `faostat_supply_utilization` | 36380 | 36380 | tonne |
| `faostat_trade` / `faostat_trade` | `faostat_trade` | 1226470 | 1226470 | 1000 USD, tonne |
| `usda_psd` / `usda_psd` | `usda_rice_psd` | 15 | 990 | 1000 MT, fraction, MT/HA, 1000 HA |
| `usda_rice_yearbook` / `usda_rice_yearbook` | `usda_rice_yearbook` | 13131 | 12936 | USD/tonne |
| `worldbank_pinksheet` / `worldbank_pinksheet` | `worldbank_commodity_monthly` | 792 | 56232 | USD/tonne, USD/kg, USD/barrel, USD/troy_oz, USD/dmtu, USD/MMBtu, USD/cubic_meter, index(2010=100), cents/sheet |
| `thitruongnongsan` / `thitruongnongsan` | `thitruongnongsan` | 20394 | 20387 | VND/kg |
| `nso_vietnam` / `nso_vietnam_v2` | `nso_rice_statistics_v2` | 833 | 23060 | hectare, tonne, percent, kg/hectare |

Total Silver: **1414527 observations**.
USDA PSD Bronze is wide; its15 source records expand to990 Silver observations.
Yearbook removes195 duplicates. World Bank excludes13,464 synthetic empty
observations. Market prices retain20,387 rows after4 quarantined conflicts and
3 duplicates. NSO13 structural source records are recorded separately as exclusions.

## Grain and business keys

- **faostat_production**: `country_code, commodity_code, year, element_code`.
- **faostat_monthly_price**: `country_code, commodity_code, year, month_code, element_code`.
- **faostat_supply_utilization**: `country_code, commodity_code, year, element_code`.
- **faostat_trade**: `reporter_country_code, partner_country_code, commodity_code, year, element_code`.
- **usda_psd**: `country, commodity, attribute, market_year`.
- **usda_rice_yearbook**: `location, commodity_class, year, reference_period, statistic_type`.
- **worldbank_pinksheet**: `commodity_code, period_date`.
- **thitruongnongsan**: `market, commodity, price_type, unit, currency, source, date`.
- **nso_vietnam_v2**: `source_table, source_geography_key, year, measure, season, statistic_kind`.

Business keys are unique in the initial independent Trino validation. Current
unchanged active-file hashes preserve that accepted content. Keep year/season,
measure, geography level, price source and currency distinctions present in the
contracts; do not sum overlapping geographic levels, yields, indices or prices.

## Schema and quality

Contracts declare nullable fields, DECIMAL/date/timestamp types, required keys,
source mappings, DQ rules and partitions. Existing target incompatibility fails
closed; it does not trigger automatic schema migration. Invalid observations go
to quarantine; warnings retain valid observations. The SUA negative Residuals
policy retains236 warnings. Market price warnings number29.

`dead_letters` has17 distinct quarantine IDs:4 `thitruongnongsan` conflict
records and13 `nso_vietnam_v2_exclusions` structural records. Structural exclusions
are not counted as invalid numeric NSO observations. Its original pipeline_run_id
and detected_at are unchanged; the strict replay check must preserve them.

## NSO normalization

NSO contains23,060 observations:22,114 numeric,946 explicit missing and5 genuine
zeros. Area is hectare (factor1000), production tonne (factor1000), yield
kg/hectare (factor100), development index percent (factor1). Annual and four
season labels remain distinct. Quantity and base-100 index are separate statistics.

Raw geography labels and candidate names remain traceable; candidate names do
not define keys. Unverified geography codes and historical boundary versions are
NULL. V06.12 has explicit national-scope metadata. No province is invented or
historically remapped. See [NSO semantics](nso_v2.md).

## Lineage and evidence

All targets retain the eight Bronze audit fields. NSO also stores the pinned
Bronze read snapshot as BIGINT. `bronze_read_snapshot_id` is not inferred from
`_source_snapshot_id`; it comes from the reader context. Physical snapshot/UUID
and per-source metrics are in the [initial acceptance report](evidence/nso_v2/phase3_final_20261017_024536/report.json).
After-shutdown file hashes and persisted task states are in the
[restart evidence](evidence/noop_restart_20261017_063554/).

Runbook: [local startup](local_runbook.md). Architecture: [components and flow](architecture.md).
