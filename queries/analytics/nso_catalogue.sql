SELECT f.source_table, f.measure, f.season, f.statistic_kind, g.source_level,
 f.unit, min(f.year_key) AS first_year, max(f.year_key) AS last_year,
 count(*) AS observations, count(f.value) AS numeric_observations,
 count(DISTINCT f.geography_key) AS geographies
FROM {{fact_nso_observation}} f JOIN {{dim_source_geography}} g ON f.geography_key=g.geography_key
GROUP BY 1,2,3,4,5,6 ORDER BY 1,2,3,4,5
