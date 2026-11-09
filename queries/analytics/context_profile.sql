SELECT f.source_system_key,s.source_id,{{profile_expression}} AS category,count(*) AS observations
FROM {{fact}} f JOIN {{dim_source_system}} s ON f.source_system_key=s.source_system_key
GROUP BY 1,2,3 ORDER BY 2,3
