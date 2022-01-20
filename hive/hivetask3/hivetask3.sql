ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE smirnovmi;

-- Задание, возможно, не очень верно сформулировано, так как по идее еще бы нужен джойн с табличкой Логов,
-- но если поджойнить то получается 0 записей. Видимо, как писали, это из-за того, что данные несогласованы

SELECT IPRegions.region, SUM(IF(Users.sex = 'male', 1, 0)), SUM(IF(Users.sex = 'female', 1, 0))
FROM Users 
INNER JOIN IPRegions ON Users.ip = IPRegions.ip
-- INNER JOIN Logs ON Logs.ip = Users.ip
GROUP BY IPRegions.region;