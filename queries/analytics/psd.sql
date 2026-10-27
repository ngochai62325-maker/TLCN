SELECT f.market_year,f.crop_year,f.geography_key,f.product_key,t.source_metric_id AS metric,
 t.gold_unit AS unit,f.value,f.observation_key,f.missing_reason,f.dq_warnings_json
FROM {{fact_usda_psd_observation}} f JOIN {{dim_observation_type}} t ON f.observation_type_key=t.observation_type_key
WHERE {{filters}} ORDER BY metric,market_year
