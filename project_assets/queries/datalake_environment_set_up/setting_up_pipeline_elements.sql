-- 1. Use the DATA_ENGINEER role and AWS credentials to create the Snowflake stage of your pipeline in your bronze schema.
USE SCHEMA ARCADE_DB.BRONZE;
CREATE OR REPLACE STAGE open_brewery_db_stg
    URL = 's3://you-bucket-name/airflow_stage/open_brewery_db/raw_api_responses'
CREDENTIALS = (
  AWS_KEY_ID = 'YOUR_AWS_KEY_ID'
  AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY'
)
FILE_FORMAT = (TYPE = 'JSON');

-- 2. Test it
LIST @open_brewery_db_stg;

-- 3. Create a table to receive the raw data of your pipeline in the bronze layer.
CREATE TABLE RAW_OPEN_BREWERY_DB_DATA IF NOT EXISTS (
    RAW_RESPONSE VARIANT
);

-- 4. Create a table in your silver layer to load the processed data.
USE SCHEMA ARCADE_DB.SILVER;
CREATE TABLE PROCESSED_OPEN_BREWERY_DB_DATA IF NOT EXISTS (
    ID STRING COMMENT 'Unique identifier for the brewery',
    NAME STRING COMMENT 'Name of the brewery',
    BREWERY_TYPE STRING COMMENT 'Type of brewery (e.g., micro, nano, brewpub)',
    ADDRESS_1 STRING COMMENT 'Primary street address',
    ADDRESS_2 STRING COMMENT 'Secondary address (nullable)',
    ADDRESS_3 STRING COMMENT 'Tertiary address (nullable)',
    CITY STRING COMMENT 'City where the brewery is located',
    STATE_PROVINCE STRING COMMENT 'State or province of the brewery',
    POSTAL_CODE STRING COMMENT 'Postal or ZIP code of the brewery',
    COUNTRY STRING COMMENT 'Country where the brewery is located',
    LONGITUDE STRING COMMENT 'Longitude coordinate of the brewery',
    LATITUDE STRING COMMENT 'Latitude coordinate of the brewery',
    PHONE STRING COMMENT 'Contact phone number of the brewery',
    WEBSITE_URL STRING COMMENT 'Website URL of the brewery',
    STATE STRING COMMENT 'State where the brewery is located',
    STREET STRING COMMENT 'Street name of the brewery address'
)
COMMENT = 'Table storing brewery information including addresses, contact details, and geographical coordinates';

-- 5. Create a gold layer aggregated view that'll showcase data of breweries quantity per type and location.
USE SCHEMA ARCADE_DB.GOLD;
CREATE OR REPLACE VIEW AGG_OPEN_BREWERY_DB_BREWERIES_BY_TYPE_AND_LOCATION AS
SELECT 
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE,
    COUNT(DISTINCT ID) AS NUM_BREWERIES
FROM 
    SILVER.PROCESSED_OPEN_BREWERY_DB_DATA
GROUP BY 
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE
ORDER BY
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE;