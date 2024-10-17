-- 1. Create a separate warehouse to process the data operations.
CREATE WAREHOUSE IF NOT EXISTS ARCADE_WH
  WITH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300  -- Automatically suspend after 5 minutes of inactivity (300 seconds)
  AUTO_RESUME = TRUE  -- Automatically resume when a query is executed
  INITIALLY_SUSPENDED = TRUE;  -- Start in a suspended state

-- 2. Create a database to store your data
CREATE DATABASE IF NOT EXISTS ARCADE_DB;

-- 3. Create the medallion schemas in the ARCADE_DB database.
USE DATABASE ARCADE_DB;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
  
-- 4. Create a data engineer role to interact with the data in the datalake.
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;

-- 5. Create an airflow role to interact with the data from airflow.
CREATE ROLE IF NOT EXISTS AIRFLOW;

-- 5. Grant access to the database to the DATA_ENGINEER and AIRFLOW role.
GRANT USAGE ON WAREHOUSE ARCADE_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE compute_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE ARCADE_WH TO ROLE AIRFLOW;
-- 6. Grant access to the warehouse and to the DATA_ENGINEER and AIRFLOW role.
GRANT USAGE ON DATABASE ARCADE_DB TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE ARCADE_DB TO ROLE AIRFLOW;

-- 7. Grant ownership to the medallion schemas to to the data engineer role.
GRANT OWNERSHIP ON SCHEMA ARCADE_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT OWNERSHIP ON SCHEMA ARCADE_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT OWNERSHIP ON SCHEMA ARCADE_DB.GOLD   TO ROLE DATA_ENGINEER;

-- 8. Grant yourself access to the DATA_ENGINEER and AIRFLOW role. 
-- Then start using the role DATA_ENGINEER to proceed setting up your environment.
GRANT ROLE DATA_ENGINEER TO USER YOU_USER_NAME;
GRANT ROLE AIRFLOW TO USER YOU_USER_NAME;

-- 9. Grant proper access to the airflow role.
GRANT USAGE ON SCHEMA ARCADE_DB.BRONZE TO ROLE AIRFLOW;
GRANT USAGE ON SCHEMA ARCADE_DB.SILVER TO ROLE AIRFLOW;
GRANT USAGE ON SCHEMA ARCADE_DB.GOLD   TO ROLE AIRFLOW;
GRANT ALL PRIVILEGES ON FUTURE  STAGES IN SCHEMA BRONZE TO ROLE AIRFLOW;
GRANT SELECT, INSERT, UPDATE, TRUNCATE, DELETE ON FUTURE TABLES IN SCHEMA BRONZE TO ROLE AIRFLOW;
GRANT SELECT, INSERT, UPDATE, TRUNCATE, DELETE ON FUTURE TABLES IN SCHEMA SILVER TO ROLE AIRFLOW;
GRANT SELECT, INSERT, UPDATE, TRUNCATE, DELETE ON FUTURE TABLES IN SCHEMA GOLD TO ROLE   AIRFLOW;

