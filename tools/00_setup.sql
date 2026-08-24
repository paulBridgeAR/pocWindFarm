-- This instructions  is for setting up the Databricks environment for the POC Wind Farm project. It creates the necessary catalog, schema, and volume for landing data.
-- After running it, upload the three data_group_*.csv files from the repo's
-- data/ folder into:
--
--     /Volumes/turbine_poc/wind_farm/landing/source_data
--
-- The generator reads from there and writes the daily files into landing/raw,
-- which is where the pipeline picks them up.

CREATE CATALOG IF NOT EXISTS turbine_poc;
CREATE SCHEMA  IF NOT EXISTS turbine_poc.wind_farm;
CREATE VOLUME  IF NOT EXISTS turbine_poc.wind_farm.landing;
