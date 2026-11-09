SELECT f.*, g.source_labels_json, g.source_level, g.interpretation_status FROM {{fact_nso_observation}} f JOIN {{dim_source_geography}} g ON f.geography_key=g.geography_key
