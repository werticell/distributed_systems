ADD JAR Reverse/target/Reverse-1.0-SNAPSHOT.jar;

CREATE TEMPORARY FUNCTION IDENTITY AS 'ru.mikhailsv.ReverseUDF';

USE smirnovmi;


SELECT reverse(ip)
FROM Subnets 
LIMIT 10;
