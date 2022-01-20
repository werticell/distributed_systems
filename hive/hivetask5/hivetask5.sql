ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE smirnovmi;

ADD FILE ./replacement.sh;

-- ip, TRANSFORM(request_body), page_size, status_code, client_info, request_ts

SELECT TRANSFORM(ip, request_ts, request_body, page_size, status_code, client_info)
USING './replacement.sh'
FROM Logs
LIMIT 10;
