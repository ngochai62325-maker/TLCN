SELECT count(*) AS rows, count(value) AS numeric_observations,
 count_if(value IS NULL) AS missing_observations, count_if(value=0) AS genuine_zero,
 count_if(dq_warnings_json IS NOT NULL AND dq_warnings_json <> '[]') AS warned_observations,
 min({{period_column}}) AS first_period, max({{period_column}}) AS last_period,
 count(DISTINCT {{period_column}}) AS represented_periods
FROM {{fact}}
