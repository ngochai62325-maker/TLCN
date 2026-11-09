SELECT f.observation_key,f.year_key,f.geography_key,f.source_table,f.source_geography_key,
 f.measure,f.season,f.statistic_kind,f.value,f.unit,f.release_status,f.quality_status,
 f.review_reason,f.diagnostic_evidence_json,f.dq_warnings_json,
 g.source_labels_json,g.source_level
FROM {{fact_nso_observation}} f JOIN {{dim_source_geography}} g ON f.geography_key=g.geography_key
WHERE {{filters}} ORDER BY f.geography_key,f.measure,f.season,f.year_key
