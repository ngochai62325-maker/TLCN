SELECT f.geography_key, f.source_geography_key, g.source_labels_json,
 f.source_table, g.source_level, f.measure, f.statistic_kind, f.season, f.unit,
 min(f.year_key) AS first_year, max(f.year_key) AS last_year,
 count(*) AS observations, count(f.value) AS numeric_observations
FROM {{fact_nso_observation}} f JOIN {{dim_source_geography}} g ON f.geography_key=g.geography_key
WHERE {{filters}}
GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY 1,6,7,8
