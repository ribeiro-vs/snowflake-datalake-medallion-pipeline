-- Inserting data into the bronze layer table.
USE SCHEMA {{params.database_name}}.BRONZE;
TRUNCATE RAW_OPEN_BREWERY_DB_DATA;
COPY INTO RAW_OPEN_BREWERY_DB_DATA
FROM @open_brewery_db_stg/open_brewery_db_response.json
FILE_FORMAT = (TYPE = 'JSON');

-- Validating the number of lines after the insertion to avoid data quality issues.
SELECT COUNT(*) AS rows_inserted
FROM RAW_OPEN_BREWERY_DB_DATA
WHERE RAW_RESPONSE IS NOT NULL;