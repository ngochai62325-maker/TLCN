SELECT f.product_key,p.source_identity,p.source_labels_json,count(*) AS observations,
 count(f.value) AS numeric_observations,min(f.{{period_column}}) AS first_year,max(f.{{period_column}}) AS last_year,
 array_agg(DISTINCT t.source_metric_id) AS metric_codes
FROM {{fact}} f JOIN {{dim_source_product}} p ON f.product_key=p.product_key
JOIN {{dim_observation_type}} t ON f.observation_type_key=t.observation_type_key
WHERE f.{{geography_column}}={{geography}}
GROUP BY 1,2,3 ORDER BY source_identity
