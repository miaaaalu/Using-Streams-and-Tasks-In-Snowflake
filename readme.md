# Data Streaming Via Snowpipe
![screenshot](DataStreamingViaSnowpipe.jpg)

```sql
--  1.create database
CREATE OR REPLACE DATABASE Snowpipe_demo;

--  2.create a staging table 
CREATE OR REPLACE TABLE Staging_Table (
    customer_id Text,
    datetime Text,
    attd Text,
    credit_score Text,
    state_id Text,
    type Text
);

--  3. Create a file format 
CREATE OR REPLACE FILE FORMAT SNOWPIPE_CSV_Format
    TYPE = 'CSV'
    skip_header = 1
    field_delimiter = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--  4. create an external S3 stage
CREATE OR REPLACE STAGE Snowpipe_Demo_Stage
    URL="S3landingbucket"
    CREDENTIALS = (AWS_KEY_ID = '***************' AWS_SECRET_KEY = '***************+***************')
    file_format = SNOWPIPE_CSV_Format;

--  5. create a snowpipe
CREATE OR REPLACE PIPE Snowpipe_Demo
    AUTO_INGEST = TRUE 
    AS COPY INTO Staging_Table 
    FROM @Snowpipe_Demo_Stage
    FILE_FORMAT = (FORMAT_NAME = SNOWPIPE_CSV_Format);

--  6.save notification_channel url for S3 Event Notification
SHOW PIPES;
arn:aws:sqs:ap-southeast-2:***************:sf-snowpipe-AIDAXCWW6DZILFHBVQMMV-C8qof2NHZLas0q_FNvbOcw

--------------Table Transformation---------

--  7. Create a Target table 
CREATE OR REPLACE TABLE Target_Table(
    customer_id Text,
    datetime datetime,
    attd Float8,
    credit_score Text,
    state_id Text,
    type Text
);

--  8. Create a View for staging table with data transformation
CREATE OR REPLACE VIEW Staging_Table_VIEW AS(
    SELECT DISTINCT
    CUSTOMER_ID,
    CONCAT((split_part(DATETIME, ' ', 0)||' '||split_part(DATETIME, ' ', 2)))::datetime AS DATETIME,
    ATTD::FLOAT8 AS ATTD,
    CREDIT_SCORE,
    STATE_ID,
    TYPE
    FROM Staging_Table
 );

--  9. Create STREAM on Staging_Table 
CREATE OR REPLACE STREAM Stream_demo ON TABLE Staging_Table;

-- 10. Create STREAM TASK. 
-- Once there is new data trigger snowpipe, snowpipe will load data into stage table, 
-- stream task will periodically merge data from stage table to the target table.
CREATE OR REPLACE TASK MERAGE_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 minute' 
WHEN SYSTEM$STREAM_HAS_DATA('Stream_demo') -- Merage into target table by stream 
AS
MERGE INTO TARGET_TABLE AS DEST
USING Staging_Table_VIEW AS SRC 
    ON src.customer_id = dest.customer_id
    AND src.datetime = dest.datetime
    AND src.attd = dest.attd
    AND src.credit_score = dest.credit_score
    AND src.state_id = dest.state_id
    AND src.type = dest.type
WHEN NOT matched 
    THEN INSERT (customer_id, datetime, attd, credit_score, state_id, type)
    VALUES (src.customer_id, src.datetime, src.attd, src.credit_score, src.state_id, src.type);
    
// 11.RUN STREAM TASK
ALTER TASK MERAGE_TASK RESUME;
```