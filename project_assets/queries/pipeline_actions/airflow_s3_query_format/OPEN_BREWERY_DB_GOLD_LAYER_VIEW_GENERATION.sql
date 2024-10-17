-- Assures the existence of an aggregate view in the gold layer.
USE SCHEMA {{params.database_name}}.GOLD;
CREATE OR REPLACE VIEW GOLD.AGG_OPEN_BREWERY_DB_BREWERIES_BY_TYPE_AND_LOCATION AS
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