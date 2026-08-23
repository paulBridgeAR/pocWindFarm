CREATE OR REFRESH MATERIALIZED VIEW silver_turbine_readings (
  CONSTRAINT valid_turbine_id
    EXPECT (turbine_id IS NOT NULL AND turbine_id BETWEEN 1 AND 15) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp
    EXPECT (event_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT power_present
    EXPECT (power_output IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT power_in_range
    EXPECT (power_output BETWEEN 0.0 AND 5.0) ON VIOLATION DROP ROW,
  CONSTRAINT wind_speed_in_range
    EXPECT (wind_speed IS NULL OR wind_speed BETWEEN 0.0 AND 40.0) ON VIOLATION DROP ROW, --Turbines cut out around 25 m/s but anemometers keep reading. 40 m/s is 90 mph — sensor fault territory
  CONSTRAINT wind_dir_in_range
    EXPECT (wind_direction IS NULL OR wind_direction BETWEEN 0 AND 359) ON VIOLATION DROP ROW
)
COMMENT "Cleaned readings. One row per turbine per hour."
TBLPROPERTIES ("quality" = "silver")
AS
WITH typed AS (
  SELECT
    CAST(turbine_id     AS INT)    AS turbine_id,
    TO_TIMESTAMP(timestamp)        AS event_ts,
    CAST(wind_speed     AS DOUBLE) AS wind_speed,
    CAST(wind_direction AS INT)    AS wind_direction,
    CAST(power_output   AS DOUBLE) AS power_output,
    source_file,
    ingest_ts
  FROM bronze_turbine_raw
)
SELECT *, TO_DATE(event_ts) AS stats_date
FROM typed
-- Grain: one row per turbine per hour. On a re-delivery the latest ingest wins.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY turbine_id, event_ts ORDER BY ingest_ts DESC
) = 1