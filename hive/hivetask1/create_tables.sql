ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

-- Logs table
USE smirnovmi;


DROP TABLE IF EXISTS LogsFull;

CREATE EXTERNAL TABLE LogsFull (
    ip STRING,
    request_ts INT,
    request_body STRING,
    page_size SMALLINT,
    status_code SMALLINT,
    client_info STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = '^(\\S*)\\t{3}(\\d{8})\\S*\\s(\\S+)\\s(\\d+)\\s(\\d+)\\s(\\S+).*$')
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=200;
SET hive.exec.max.dynamic.partitions.pernode=200;

DROP TABLE IF EXISTS Logs;

CREATE EXTERNAL TABLE Logs (
    ip STRING,
    request_body STRING,
    page_size SMALLINT,
    status_code SMALLINT,
    client_info STRING
)
PARTITIONED BY (request_ts INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (request_ts)
SELECT ip, request_body, page_size, status_code, client_info, request_ts FROM LogsFull;


-- Users table
DROP TABLE IF EXISTS Users;

CREATE EXTERNAL TABLE Users (
    ip STRING,
    browser STRING,
    sex STRING,
    age SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = '^(\\S+)\\s(\\S+)\\s(\\S+)\\s(\\S+).*$')
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

-- IPRegions table

DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions (
    ip STRING,
    region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = '^(\\S+)\\s(\\S+).*$')
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

-- Subnets table

DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = '^(\\S+)\\s(\\S+).*$')
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';


-- Selects

SELECT * FROM Logs LIMIT 10;
SELECT * FROM Users LIMIT 10;
SELECT * FROM IPRegions LIMIT 10;
SELECT * FROM Subnets LIMIT 10;
