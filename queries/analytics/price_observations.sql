SELECT f.observation_key,f.{{period_column}} AS period,{{reference_period}} AS reference_period,
 f.price_series_key,f.value,f.missing_reason,f.dq_warnings_json,
 s.series_identity_json,s.unit,s.currency,s.price_kind,s.information_source
FROM {{fact}} f JOIN {{dim_price_series}} s ON f.price_series_key=s.price_series_key
WHERE {{filters}} ORDER BY period,reference_period
