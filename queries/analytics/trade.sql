SELECT q.year_key,q.reporter_geography_key,q.partner_geography_key,q.product_key,
 q.{{quantity}} AS quantity_t,q.{{monetary}} AS value_usd,q.{{ratio}} AS reported_unit_value_usd_t,
 q.{{present_quantity}} AS present_quantity,q.{{present_value}} AS present_value,
 q.{{observation_quantity}} AS observation_quantity,q.{{observation_value}} AS observation_value,
 g.source_identity AS partner_identity,g.source_labels_json AS partner_labels,g.source_level AS partner_level
FROM ({{semantic}}) q JOIN {{dim_source_geography}} g ON q.partner_geography_key=g.geography_key
ORDER BY year_key,partner_geography_key
