SELECT q.*,g.source_identity AS geography_identity,g.source_labels_json AS geography_labels,
 p.source_identity AS product_identity,p.source_labels_json AS product_labels,
 CAST(NULL AS varchar) AS production_source_flag,
 CAST(NULL AS varchar) AS area_source_flag,
 CAST(NULL AS varchar) AS reported_yield_source_flag
FROM ({{semantic}}) q JOIN {{dim_source_geography}} g ON q.geography_key=g.geography_key
JOIN {{dim_source_product}} p ON q.product_key=p.product_key
ORDER BY q.geography_key,q.year_key
