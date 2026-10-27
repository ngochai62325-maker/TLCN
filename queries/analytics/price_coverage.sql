SELECT f.price_series_key,s.series_identity_json,s.unit,s.currency,s.price_kind,
 count(*) AS observations,count(f.value) AS numeric_observations,
 min(f.{{period_column}}) AS first_period,max(f.{{period_column}}) AS last_period,
 count(DISTINCT f.{{period_column}}) AS represented_periods
FROM {{fact}} f JOIN {{dim_price_series}} s ON f.price_series_key=s.price_series_key
GROUP BY 1,2,3,4,5 ORDER BY observations DESC,price_series_key
