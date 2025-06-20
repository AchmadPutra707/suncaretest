USE WAREHOUSE WH_DEV;

USE ROLE SF_DEV_WRITE;

USE DATABASE SUNCARE_TEST;

CREATE SCHEMA IF NOT EXISTS STOCKS;

USE SCHEMA STOCKS;

CREATE OR REPLACE TABLE STOCKS.STOCKS_PRICES (   //Create table for contain stock price downloaded into stage
    timestamp STRING,
    symbol STRING,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    VOLUME INT

);

CREATE OR REPLACE STAGE ALPHA_VANTAGE_STAGE        // create stages to be put file from downloaded
FILE_FORMAT = (TYPE = JSON);

// put json filed downloaded from third party API into stages
PUT file://C:/Downloads/stock_data.json @ALPHA_VANTAGE_STAGE;

LIST @ALPHA_VANTAGE_STAGE;

CREATE OR REPLACE TABLE RAW_STOCK_DATA(data Variant); // temp store json as semi structured file 

SELECT * FROM RAW_STOCK_DATA;

COPY INTO RAW_STOCK_DATA                    // store it from stages to staging table
FROM @ALPHA_VANTAGE_STAGE
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = CONTINUE;

CREATE OR REPLACE TABLE STOCKS.HISTORICAL_PRICES (

    timestamp STRING,
    symbol STRING,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    VOLUME INT

);

SHOW TABLES;

INSERT INTO STOCKS_PRICES
SELECT 
    (SELECT data:"Meta Data":"2. Symbol"::STRING FROM raw_stock_data) AS symbol, 
    time_series.key::STRING AS timestamp, 
    time_series.value:"1. open"::FLOAT AS open_price, 
    time_series.value:"2. high"::FLOAT AS high_price, 
    time_series.value:"3. low"::FLOAT AS low_price, 
    time_series.value:"4. close"::FLOAT AS close_price, 
    time_series.value:"5. volume"::INT AS volume
FROM raw_stock_data, 
    LATERAL FLATTEN(input => data:"Time Series (5min)") time_series;


SELECT * FROM STOCKS_PRICES;

TRUNCATE TABLE STOCKS_PRICES;

CREATE TASK AUTO_INGEST_STOCKS
WAREHOUSE = WH_DEV
SCHEDULE = '5 MINUTE'
AS
BEGIN 
    INSERT INTO STOCKS_PRICES
    SELECT 
    (SELECT data:"Meta Data":"2. Symbol"::STRING FROM raw_stock_data) AS symbol, 
    time_series.key::STRING AS timestamp, 
    time_series.value:"1. open"::FLOAT AS open_price, 
    time_series.value:"2. high"::FLOAT AS high_price, 
    time_series.value:"3. low"::FLOAT AS low_price, 
    time_series.value:"4. close"::FLOAT AS close_price, 
    time_series.value:"5. volume"::INT AS volume
FROM raw_stock_data, 
    LATERAL FLATTEN(input => data:"Time Series (5min)") time_series;
END;

SHOW TASKS; // see task status

ALTER TASK AUTO_INGEST_STOCKS RESUME; //begin the tasks 

ALTER TASK AUTO_INGEST_STOCKS SUSPEND; //suspend the tasks

SELECT * FROM snowflake.account_usage.task_history //need ACCOUNTADMIN Access
WHERE TASK_NAME = 'AUTO_INGEST_STOCKS'
ORDER BY SCHEDULED_TIME DESC; 

SELECT * FROM HISTORICAL_PRICES;

TRUNCATE TABLE HISTORICAL_PRICES;

CREATE OR REPLACE PROCEDURE UPSERT_STOCK_DATA()
RETURNS STRING 
LANGUAGE SQL 
AS
BEGIN

    MERGE INTO HISTORICAL_PRICES AS hp
    USING STOCKS_PRICES AS sp
    ON hp.symbol = sp.symbol AND hp.timestamp = sp.timestamp

    WHEN MATCHED THEN
        UPDATE SET 
            open_price = sp.open_price,
            high_price = sp.high_price,
            low_price = sp.low_price,
            close_price = sp.close_price,
            volume = sp.volume

    WHEN NOT MATCHED THEN
        INSERT (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES (sp.symbol, sp.timestamp, sp.open_price, sp.high_price, sp.low_price, sp.close_price, sp.volume);

    RETURN 'Upsert is successful';
END;


CALL UPSERT_STOCK_DATA();

CREATE TASK 

//Basic procedure
CREATE OR REPLACE PROCEDURE output_message()
RETURNS VARCHAR
LANGUAGE SQL
AS 
$$
DECLARE VAR1 STRING DEFAULT 'Hello world';
BEGIN
    -- Using dynamic SQL to create the table
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE Testpce AS SELECT ''' || VAR1 || ''' AS COL1';
    RETURN VAR1;
END;
$$
;

CALL output_message();

//Flatten table 
