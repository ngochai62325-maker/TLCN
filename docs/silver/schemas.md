> Historical/legacy reference. Current local nine-source routing, NSO V2 and acceptance are documented in [README](README.md), [data catalog](data_catalog.md) and [validation](validation.md). Original bytes are preserved in [archive](archive/manifest.json).

# Bronze / Silver schemas

Generated from 9 executable YAML contracts; updated 2026-10-11.

Bronze columns use the ingestion sanitizer. Derived sources are produced by source preprocessing.

## faostat_monthly_price

Status: **READY**.

Bronze: `iceberg.bronze.faostat_monthly_price`; Silver: `iceberg.silver.faostat_monthly_price`.

**Business key:** `country_code`, `commodity_code`, `year`, `month_code`, `element_code`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `domain_code` | `Domain Code` | `string` | false | FAOSTAT domain code |
| `domain` | `Domain` | `string` | false | FAOSTAT domain description |
| `country_code` | `Area Code (M49)` | `string` | false | M49 Area Code |
| `country_name` | `Area` | `string` | false | Country or Area name |
| `element_code` | `Element Code` | `string` | false | Indicator element code (e.g., 5530 for Producer Price (LCU/tonne)) |
| `element` | `Element` | `string` | false | Indicator description |
| `commodity_code` | `Item Code (CPC)` | `string` | false | CPC Item Code |
| `commodity_name` | `Item` | `string` | false | Commodity description |
| `year` | `Year` | `integer` | false | Reference year |
| `month_code` | `Months Code` | `string` | false | Month code (e.g., 7021 for Annual value, 7001 for Jan) |
| `month_name` | `Months` | `string` | false | Month description |
| `unit` | `Unit` | `string` | true | Unit of measurement (e.g., LCU, USD, SLC) |
| `value` | `Value` | `decimal(18,4)` | true | The actual price value |
| `flag` | `Flag` | `string` | true | Observation status flag |
| `flag_description` | `Flag Description` | `string` | true | Description of the observation status flag |
| `time_grain` | `time_grain` | `string` | false | annual or monthly |
| `month` | `month` | `integer` | true | 1..12 for monthly; NULL for annual |
| `currency` | `currency` | `string` | true | LCU/SLC/USD; NULL for price index |
| `price_kind` | `price_kind` | `string` | false | producer_price or producer_price_index |

## faostat_production

Status: **READY**.

Bronze: `iceberg.bronze.faostat_production`; Silver: `iceberg.silver.faostat_production`.

**Business key:** `country_code`, `commodity_code`, `year`, `element_code`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `domain_code` | `Domain Code` | `string` | false | FAOSTAT domain code |
| `domain` | `Domain` | `string` | false | FAOSTAT domain description |
| `country_code` | `Area Code (M49)` | `string` | false | M49 Area Code |
| `country_name` | `Area` | `string` | false | Country or Area name |
| `element_code` | `Element Code` | `string` | false | Indicator element code (e.g., 5312 for Area harvested, 5510 for Production) |
| `element` | `Element` | `string` | false | Indicator description |
| `commodity_code` | `Item Code (CPC)` | `string` | false | CPC Item Code |
| `commodity_name` | `Item` | `string` | false | Commodity description |
| `year` | `Year` | `integer` | false | Reference year |
| `unit` | `Unit` | `string` | true | Unit of measurement (ha, kg/ha, t, etc.) |
| `value` | `Value` | `decimal(18,2)` | true | The actual production or area value |
| `flag` | `Flag` | `string` | true | Observation status flag |
| `flag_description` | `Flag Description` | `string` | true | Description of the observation status flag |

## faostat_supply_utilization

Status: **READY**.

Bronze: `iceberg.bronze.faostat_supply_utilization`; Silver: `iceberg.silver.faostat_supply_utilization`.

**Business key:** `country_code`, `commodity_code`, `year`, `element_code`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `domain_code` | `Domain Code` | `string` | false | FAOSTAT domain code |
| `domain` | `Domain` | `string` | false | FAOSTAT domain description |
| `country_code` | `Area Code (M49)` | `string` | false | M49 Area Code |
| `country_name` | `Area` | `string` | false | Country or Area name |
| `element_code` | `Element Code` | `string` | false | Indicator element code |
| `element` | `Element` | `string` | false | Indicator description |
| `commodity_code` | `Item Code (CPC)` | `string` | false | CPC Item Code |
| `commodity_name` | `Item` | `string` | false | Commodity description |
| `year` | `Year` | `integer` | false | Reference year |
| `unit` | `Unit` | `string` | true | Unit of measurement (mostly tonnes) |
| `value` | `Value` | `decimal(18,2)` | true | Quantity value |
| `flag` | `Flag` | `string` | true | Observation status flag |
| `flag_description` | `Flag Description` | `string` | true | Description of the observation status flag |
| `note` | `Note` | `string` | true | Additional notes |

## faostat_trade

Status: **READY**.

Bronze: `iceberg.bronze.faostat_trade`; Silver: `iceberg.silver.faostat_trade`.

**Business key:** `reporter_country_code`, `partner_country_code`, `commodity_code`, `year`, `element_code`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `reporter_country_name` | `Reporter Countries` | `string` | false | Reporting country name |
| `reporter_country_code` | `Reporter Country Code (M49)` | `string` | false | M49 code for reporter country |
| `partner_country_name` | `Partner Countries` | `string` | false | Partner country name |
| `partner_country_code` | `Partner Country Code (M49)` | `string` | false | M49 code for partner country |
| `commodity_name` | `Item` | `string` | false | Commodity description |
| `commodity_code` | `Item Code (CPC)` | `string` | false | CPC item code |
| `element_name` | `Element` | `string` | false | Element description (e.g., Import quantity, Export value) |
| `element_code` | `Element Code` | `string` | false | Element code |
| `year` | `Year` | `integer` | false | Reference year |
| `unit` | `Unit` | `string` | true | Unit of measurement (e.g., t, 1000 USD) |
| `value` | `Value` | `decimal(18,2)` | true | Trade quantity or value |
| `flag` | `Flag` | `string` | true | Observation status flag |

## nso_vietnam

Status: **READY**.

Bronze: `iceberg.bronze.nso_vietnam`; Silver: `iceberg.silver.nso_rice_statistics`.

**Business key:** `source_table`, `geography_name_raw`, `year`, `season`, `measure`, `statistic_kind`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `source_table` | `source_table` | `string` | false | Derived NSO source observation |
| `source_column` | `source_column` | `string` | false | Derived NSO source observation |
| `geography_name_raw` | `geography_name_raw` | `string` | false | Derived NSO source observation |
| `geography_level` | `geography_level` | `string` | false | Derived NSO source observation |
| `geography_name` | `geography_name` | `string` | true | Exact candidate name with restored accents; historical code mapping still needs review |
| `geography_code` | `geography_code` | `string` | true | Derived NSO source observation |
| `mapping_status` | `mapping_status` | `string` | false | Derived NSO source observation |
| `country_code` | `country_code` | `string` | false | Derived NSO source observation |
| `province_code` | `province_code` | `string` | true | Derived NSO source observation |
| `commodity_name` | `commodity_name` | `string` | false | Derived NSO source observation |
| `period_raw` | `period_raw` | `string` | false | Derived NSO source observation |
| `year` | `year` | `integer` | false | Derived NSO source observation |
| `period_valid` | `period_valid` | `boolean` | false | Derived NSO source observation |
| `is_provisional` | `is_provisional` | `boolean` | false | Derived NSO source observation |
| `season` | `season` | `string` | false | Derived NSO source observation |
| `measure` | `measure` | `string` | false | Derived NSO source observation |
| `statistic_raw` | `statistic_raw` | `string` | false | Derived NSO source observation |
| `statistic_kind` | `statistic_kind` | `string` | false | Derived NSO source observation |
| `source_unit` | `source_unit` | `string` | false | Derived NSO source observation |
| `unit` | `unit` | `string` | false | Derived NSO source observation |
| `conversion_factor` | `conversion_factor` | `decimal(18,6)` | false | Derived NSO source observation |
| `unit_source_reference` | `unit_source_reference` | `string` | false | Derived NSO source observation |
| `value_raw` | `value_raw` | `string` | true | Derived NSO source observation |
| `value_source` | `value_source` | `decimal(28,8)` | true | Derived NSO source observation |
| `value` | `value` | `decimal(28,8)` | true | Derived NSO source observation |
| `is_missing` | `is_missing` | `boolean` | false | Derived NSO source observation |
| `missing_kind` | `missing_kind` | `string` | false | Derived NSO source observation |

## thitruongnongsan

Status: **READY**.

Bronze: `iceberg.bronze.thitruongnongsan`; Silver: `iceberg.silver.thitruongnongsan`.

**Business key:** `market`, `commodity`, `price_type`, `date`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `commodity` | `Tên_mặt_hàng` | `string` | false | Name of the rice variety or commodity |
| `market` | `Thị_trường` | `string` | false | Market location (usually a province or specific market in Vietnam) |
| `price_type` | `Loại_giá` | `string` | false | Type of price (e.g., Wholesale, Retail) |
| `unit` | `Đơn_vị_tính` | `string` | true | Unit of measurement (e.g., VNĐ/kg) |
| `currency` | `Loại_tiền` | `string` | true | Currency type (e.g., VND) |
| `source` | `Nguồn` | `string` | true | Source of the price information |
| `date` | `Ngày` | `date` | false | Date of the price observation |
| `price_value` | `Giá` | `decimal(18,2)` | true | The actual price value |
| `year` | `year` | `integer` | false | Calendar year derived from parsed date; used for partition pruning |

## usda_psd

Status: **READY**.

Bronze: `iceberg.bronze.usda_psd`; Silver: `iceberg.silver.usda_rice_psd`.

**Business key:** `country`, `commodity`, `attribute`, `market_year`.

**Partition columns:** `market_year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `country` | `country` | `string` | false | Reporting country name (e.g., Vietnam) |
| `commodity` | `commodity` | `string` | false | Agricultural commodity (e.g., Rice, Milled) |
| `attribute` | `attribute` | `string` | false | Balance sheet attribute (e.g., Area Harvested, Production, Ending Stocks) |
| `market_year` | `market_year` | `integer` | false | Market year beginning year (e.g., 1960 for 1960/1961) |
| `crop_year` | `crop_year` | `string` | false | Crop year string (e.g., '1960/1961') strictly preserved from source definition |
| `unit` | `unit_description` | `string` | true | Unit of measurement (e.g., 1000 MT, 1000 HA, MT/HA) |
| `value` | `value` | `decimal(18,4)` | true | Balance sheet quantity or dimensionless milling fraction; see unit |
| `value_source` | `value_source` | `decimal(28,8)` | true | Numeric source value before conversion |
| `source_unit` | `source_unit` | `string` | true | Original unit label, including known Milling Rate mislabel |
| `conversion_factor` | `conversion_factor` | `decimal(18,6)` | false | 0.0001 for exact Milling Rate (.9999), otherwise 1 |

## usda_rice_yearbook

Status: **READY**.

Bronze: `iceberg.bronze.usda_rice_yearbook`; Silver: `iceberg.silver.usda_rice_yearbook`.

**Business key:** `location`, `commodity_class`, `year`, `reference_period`, `statistic_type`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `table_name` | `TABLE_NAME` | `string` | true | Source table name |
| `table_number` | `TABLE_NUMBER` | `integer` | true | Source table number |
| `commodity` | `COMMODITY_DESCRIPTION` | `string` | false | Commodity description (usually RICE) |
| `commodity_class` | `CLASS_DESCRIPTION` | `string` | false | Class or variety of rice (e.g., 5-PERCENT BROKEN, FRAGRANT WHITE RICE) |
| `year` | `YEAR` | `integer` | false | Reference year |
| `reference_period` | `REFERENCE_PERIOD_DESCRIPTION` | `string` | false | Reference period, usually a Month (e.g., August) |
| `location` | `LOCATION_DESCRIPTION` | `string` | false | Location/Country of the export price (e.g., THAILAND, VIETNAM) |
| `statistic_type` | `STATISTIC_DESCRIPTION` | `string` | false | Type of statistic (e.g., FREIGHT ON BOARD PRICE) |
| `value` | `VALUE` | `decimal(18,2)` | true | Price value |
| `unit` | `UNIT_DESCRIPTION` | `string` | true | Unit of measurement (e.g., DOLLARS PER METRIC TON) |

## worldbank_pinksheet

Status: **READY**.

Bronze: `iceberg.bronze.worldbank_pinksheet`; Silver: `iceberg.silver.worldbank_commodity_monthly`.

**Business key:** `commodity_code`, `period_date`.

**Partition columns:** `year`.

| Output column | Bronze / derived source | Spark type | Nullable | Meaning |
|---|---|---|---|---|
| `commodity_code` | `commodity_code` | `string` | false | Standardized commodity identifier (e.g., rice_thai_5, rice_viet_namese_5, maize, wheat) |
| `commodity_name` | `commodity_name` | `string` | false | Human-readable commodity description |
| `period_code` | `period` | `string` | false | Source period string (e.g., 2024M08) |
| `period_date` | `period_date` | `date` | false | First day of the reference month (e.g., 2024-08-01) |
| `year` | `year` | `integer` | false | Calendar year |
| `month` | `month` | `integer` | false | Calendar month (1-12) |
| `price` | `price` | `decimal(18,2)` | true | Monthly commodity benchmark price |
| `unit` | `unit` | `string` | true | Unit of price measurement (e.g., $/mt, $/kg, $/bbl) |
| `series_name` | `series_name` | `string` | true | World Bank series description |

## Audit fields shared by Silver tables

Eight Bronze audit fields are propagated when present; their existing source types are retained.

| Field | Typical Bronze/Spark type | Meaning |
|---|---|---|
| `_ingestion_run_id` | string | Bronze execution ID |
| `_ingestion_batch_id` | string | Source batch |
| `_ingestion_chunk_id` | bigint | Source chunk |
| `_ingestion_timestamp` | timestamp | UTC ingestion time used for freshness |
| `_source_id` | string | Registry source ID |
| `_source_file` | string | Original source artifact |
| `_source_checksum` | string | Source checksum |
| `_source_snapshot_id` | bigint | Metadata snapshot reference |
| `_dq_warnings` | array<struct<rule_id:string,error_message:string,failed_column:string>> | Nonblocking failures |
| `_silver_processed_at` | timestamp | UTC last effective Silver processing time |

Contract nullable rules are enforced through row-level DQ. Iceberg output fields may remain physically nullable; required columns are checked by the post-write audit.

## Quarantine schema

Table: `iceberg.silver.dead_letters`.

| Column | Spark type | Meaning |
|---|---|---|
| `quarantine_record_id` | string | SHA-256(dataset + JSON payload) |
| `source_dataset` | string | Contract dataset ID |
| `pipeline_run_id` | string | Silver execution ID |
| `detected_at` | timestamp | Latest detection time in UTC |
| `business_key_hash` | string | SHA-256 of explicit-null JSON key struct |
| `errors` | array<struct<rule_id:string,error_message:string,failed_column:string>> | Blocking rule failures |
| `payload` | string | JSON normalized record, source diagnostics, original `_source_payload` and lineage |

Quarantine is idempotent for an identical dataset and payload. A re-ingested row with different Bronze audit metadata is a distinct payload and may create a distinct dead letter; replaying the same Bronze snapshot updates the existing record.

Legacy precreated tables can retain additional nullable columns; the acceptance evidence includes their full physical schemas. NSO exclusions use the shared quarantine table with `source_dataset=nso_vietnam_exclusions`.
