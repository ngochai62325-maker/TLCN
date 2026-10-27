SELECT {{period_column}} AS period, count(*) AS observations, count(value) AS numeric_observations
FROM {{fact}} GROUP BY {{period_column}} ORDER BY {{period_column}}
