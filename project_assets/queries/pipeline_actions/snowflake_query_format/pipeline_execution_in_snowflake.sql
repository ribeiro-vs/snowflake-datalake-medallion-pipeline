-- 1. Inserting data into the bronze layer table;
USE SCHEMA ARCADE_DB.BRONZE;
TRUNCATE RAW_OPEN_BREWERY_DB_DATA;
COPY INTO RAW_OPEN_BREWERY_DB_DATA
FROM @open_brewery_db_stg/open_brewery_db_response.json
FILE_FORMAT = (TYPE = 'JSON');

-- 2. Inserting data into the bronze layer table.
-- Note: The "INSERT OVERWRITE INTO" statement is ACID-compliant, meaning the operation is atomic and transactional. 
-- If any errors occur during execution, no changes will be committed, and an exception will be raised.
-- This ensures data integrity, prevents partial modifications, and maintains data quality.
USE SCHEMA ARCADE_DB.SILVER;
INSERT OVERWRITE INTO PROCESSED_OPEN_BREWERY_DB_DATA 
SELECT
    f.value:id::STRING AS id,
    f.value:name::STRING AS name,
    f.value:brewery_type::STRING AS brewery_type,
    f.value:address_1::STRING AS address_1,
    f.value:address_2::STRING AS address_2,
    f.value:address_3::STRING AS address_3,
    f.value:city::STRING AS city,
    f.value:state_province::STRING AS state_province,
    f.value:postal_code::STRING AS postal_code,
    f.value:country::STRING AS country,
    f.value:longitude::STRING AS longitude,
    f.value:latitude::STRING AS latitude,
    f.value:phone::STRING AS phone,
    f.value:website_url::STRING AS website_url,
    f.value:state::STRING AS state,
    f.value:street::STRING AS street
FROM 
    BRONZE.RAW_OPEN_BREWERY_DB_DATA,
    LATERAL FLATTEN(input => RAW_RESPONSE) f;