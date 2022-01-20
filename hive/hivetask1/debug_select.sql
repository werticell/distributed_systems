ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;


USE smirnovmi_test;


SELECT request_ts, COUNT(ip) AS visits_cnt
FROM Logs
GROUP BY request_ts
ORDER BY visits_cnt DESC;