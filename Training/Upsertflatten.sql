USE DATABASE SUNCARE_TEST;

USE SCHEMA CALOUNDRA;

USE ROLE SF_DEV_WRITE;

PUT file://C:/Downloads/users.json @SUNSHINE_STAGE;

LIST@SUNSHINE_STAGE; 

CREATE OR REPLACE TABLE RAW_USER_DATA( data variant);

COPY INTO RAW_USER_DATA
FROM @SUNSHINE_STAGE/users.json.gz
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = CONTINUE;

SELECT * FROM RAW_USER_DATA;

SELECT *
FROM RAW_USER_DATA,
LATERAL FLATTEN(input => data:users)f;

SELECT
    f.value:id::INT AS user_id,
    f.value:name::STRING AS name,
    f.value:email::STRING AS email,
    f.value:age::INT AS age,
    f.value:location.city::STRING AS city,
    f.value:location.country::STRING AS country
FROM RAW_USER_DATA,
    LATERAL FLATTEN(input =>data:users)f;
