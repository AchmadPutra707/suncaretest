USE WAREHOUSE WH_DEV;

USE ROLE SF_DEV_WRITE;

USE SCHEMA CALOUNDRA;

LIST @STG_SUNCARE_TEST_AZURE;

SELECT * FROM USERS;

SELECT SUM(FANS) ALL_FANS, 
JOINED_DATE 
FROM USERS 
GROUP BY JOINED_DATE; 

SELECT OBJECT_CONSTRUCT(*) OUTPUT FROM USERS; // Object construct for json file . you cant download it

SELECT OBJECT_CONSTRUCT                             //json with nested queries
('Max Fans', (SELECT MAX(FANS) FROM USERS),
'Usernames',(SELECT ARRAY_AGG(NAME) FROM USERS)
) Output;

WITH FANS_BY_YEAR
AS (SELECT SUM(FANS) ALL_FANS, JOINED_DATE AS DATE // more complex queries based on temp table 
FROM USERS
GROUP BY JOINED_DATE)
SELECT OBJECT_CONSTRUCT(*) FROM FANS_BY_YEAR
ORDER BY DATE DESC;


COPY INTO @SUNSHINE_STAGE/Users_json_headers2.json //remember to unload json fila, json has to be specified
FROM (
    SELECT OBJECT_CONSTRUCT(*) FROM USERS
    ORDER BY JOINED_DATE
)
FILE_FORMAT = (
    TYPE = JSON
    COMPRESSION = NONE  
)
HEADER = TRUE
SINGLE=TRUE
OVERWRITE =TRUE;

LIST@STG_SUNCARE_TEST_AZURE;

LIST@SUNSHINE_STAGE;

// download to local file, speciy json extenstion to be recognized as json 
GET @SUNSHINE_STAGE/Users_json_headers2.json file://C:/Downloads/;    

COPY INTO @STG_SUNCARE_TEST_AZURE/Users_parquet_headers
 FROM USERS
 FILE_FORMAT = (TYPE = PARQUET)
 HEADER = TRUE;
 






