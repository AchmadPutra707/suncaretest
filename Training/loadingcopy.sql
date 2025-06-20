USE WAREHOUSE WH_DEV;

USE ROLE SF_DEV_WRITE;

USE SCHEMA MAROOCHYDORE;

CREATE OR REPLACE TABLE USERS(
    USER_ID VARCHAR(100),
    NAME VARCHAR(100),
    FANS NUMBER (38,4),
    JOINED_DATE DATE
);

CREATE OR REPLACE TABLE TEMPUSERS(
    USER_ID VARCHAR(100),
    NAME VARCHAR(100),
    FANS NUMBER (38,4),
    JOINED_DATE DATE
);

INSERT INTO TEMPUSERS (user_id,name,fans,joined_date) VALUES
('1','Kira Yamato', 100, '2025-05-15' ),
('2','Ashuran Zara', 200, '2025-05-14' ),
('3','Shin Asuka', 300, '2025-05-13' ),
('4', NULL, NULL, NULL);


SELECT * FROM TEMPUSERS;

TRUNCATE TABLE TEMPUSERS;

CREATE OR ALTER STAGE STG_SUNCARE_TEST_AZURE // Creating external stage
    COMMENT = 'External Stage, Azure Storage Account and Container: suncareapistage:\suncare-test'
    STORAGE_INTEGRATION = AZURE_SUNCAREAPISTAGE
    URL = 'azure://suncaresfdev.blob.core.windows.net/suncare-test' 
    FILE_FORMAT = (TYPE = CSV);

SHOW STAGES;

DESCRIBE STAGE STG_SUNCARE_TEST_AZURE; 

COPY INTO @STG_SUNCARE_TEST_AZURE  // copy into extenal stages from temp table to create dummy file 
FROM CALOUNDRA.TEMPUSERS; 

COPY INTO @STG_SUNCARE_TEST_AZURE/dummy_users2.csv  // copy into extenal stages from temp table to create dummy file with file name EXACTLY
FROM CALOUNDRA.TEMPUSERS
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER =1, COMPRESSION = NONE, EMPTY_FIELD_AS_NULL = true, FIELD_OPTIONALLY_ENCLOSED_BY='"')
SINGLE = TRUE;  // order snowflake to not seperate files

REMOVE @STG_SUNCARE_TEST_AZURE;

LIST @STG_SUNCARE_TEST_AZURE; // List files availkble in the external stages

SELECT * FROM USERS;

COPY INTO USERS                                // load from available file in external stage where it contains 'dummy csv'
FROM @STG_SUNCARE_TEST_AZURE/dummy_users.csv
//PATTERN = '.*dummy.*';
FILE_FORMAT = (TYPE = CSV, DATE_FORMAT = 'YYYY-MM-DD')
ON_ERROR = CONTINUE;

TRUNCATE TABLE USERS; // remove value from table user 