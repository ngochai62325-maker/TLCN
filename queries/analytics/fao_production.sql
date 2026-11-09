SELECT q.*,g.source_identity AS geography_identity,g.source_labels_json AS geography_labels,
 p.source_identity AS product_identity,p.source_labels_json AS product_labels,
 json_extract_scalar(po.source_context_json,'$.flag') AS production_source_flag,
 json_extract_scalar(ao.source_context_json,'$.flag') AS area_source_flag,
 json_extract_scalar(yo.source_context_json,'$.flag') AS reported_yield_source_flag
FROM ({{semantic}}) q JOIN {{dim_source_geography}} g ON q.geography_key=g.geography_key
JOIN {{dim_source_product}} p ON q.product_key=p.product_key
LEFT JOIN {{filtered_fact}} po ON q.observation_5510=po.observation_key
LEFT JOIN {{filtered_fact}} ao ON q.observation_5312=ao.observation_key
LEFT JOIN {{filtered_fact}} yo ON q.observation_5412=yo.observation_key
ORDER BY q.geography_key,q.year_key
