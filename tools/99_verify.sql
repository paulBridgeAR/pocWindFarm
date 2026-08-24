-- Run this after the pipeline has processed the full month.
-- The expected numbers are what I got on my run with seed 79.

-- Row counts through the layers.
-- Bronze is what landed. Silver is what survived the expectations, so the gap
-- between them is exactly the rows my cleaning rejected.
SELECT 'bronze'  AS layer, COUNT(*) AS rows FROM turbine_poc.wind_farm.bronze_turbine_raw       -- 11112
UNION ALL
SELECT 'silver',  COUNT(*)          FROM turbine_poc.wind_farm.silver_turbine_readings          -- 10962
UNION ALL
SELECT 'gold_stats', COUNT(*)       FROM turbine_poc.wind_farm.gold_turbine_daily_stats         -- 465
UNION ALL
SELECT 'gold_anomalies', COUNT(*)   FROM turbine_poc.wind_farm.gold_turbine_anomalies;          -- 17

-- 465 = 15 turbines x 31 days, so every turbine-day is present.
-- 11112 - 10962 = 150 rejected, which matches the 57 nulls + 93 out-of-range
-- the generator injected.


-- The two faults I injected deliberately, both on 12 March.
-- They show up in different places, which is the point of having both.

-- Turbine 7 underperformed for 12 hours. The values are all legal, so silver
-- lets them through and only the daily average gives it away.
SELECT * FROM turbine_poc.wind_farm.gold_turbine_anomalies
WHERE stats_date = '2022-03-12';                    -- turbine 7, z = -3.035

-- Turbine 3 lost 6 hours to a sensor outage. Its average is normal, so it is
-- not an anomaly - the problem shows in completeness instead.
SELECT * FROM turbine_poc.wind_farm.gold_turbine_daily_stats
WHERE stats_date = '2022-03-12' AND turbine_id = 3; -- 18 readings, 75%


-- Which expectations actually fired, per rule per run.
-- This is the part I did not have to build: the pipeline records it for me.
SELECT
  timestamp,
  expectation.name,
  expectation.passed_records,
  expectation.failed_records
FROM event_log(TABLE(turbine_poc.wind_farm.silver_turbine_readings)),
     LATERAL (
       SELECT explode(from_json(
         details:flow_progress.data_quality.expectations,
         'array<struct<name:string,passed_records:int,failed_records:int>>'
       )) AS expectation
     )
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
