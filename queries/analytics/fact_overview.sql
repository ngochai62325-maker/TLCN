SELECT count(*) AS rows, count(value) AS numeric_observations,
 count_if(value IS NULL) AS missing_observations, count_if(value=0) AS genuine_zero,
 0 AS warned_observations,
 min({{period_column}}) AS first_period, max({{period_column}}) AS last_period,
 count(DISTINCT {{period_column}}) AS represented_periods
FROM {{fact}}
