-- Select the pollutant levels and orders based on ascending pollutant levels
select stateName as State_Name,
count(*) as Nr_Monitors
from ( select DISTINCT stateName, countyCode, siteNum from logs ) table1
GROUP BY stateName
ORDER BY Nr_Monitors DESC;