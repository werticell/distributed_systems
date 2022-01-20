ADD JAR PossibleIps/target/PossibleIps-1.0-SNAPSHOT.jar;

USE smirnovmi;

CREATE TEMPORARY FUNCTION PossibleIps as 'ru.mikhailsv.PossibleIpsUDTF';


SELECT PossibleIps(ip, mask)
FROM Subnets
LIMIT 100;