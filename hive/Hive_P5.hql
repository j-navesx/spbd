drop database if exists project_db cascade ;
create database project_db ;
use project_db ;

CREATE TABLE logs(
    stateCode string,
    countyCode string,
    siteNum string,
    parameterCode string,
    poc string,
    latitude float,
    longitude float,
    datum string,
    parameterName string,
    sampleDuration string,
    pollutantStandard string,
    dateLocal string,
    unitsMeasure string,
    eventType string,
    observationCount string,
    observationPercent string,
    arithmeticMean float,
    firstMaxDay float,
    firstMaxHour float,
    AQI string,
    methodCode string,
    methodMode string,
    siteName string,
    address string,
    stateName string,
    countyName string,
    cityName string,
    cbsaName string,
    dateLastChange string)
    row format delimited
    fields terminated by ','
    TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data into table from localfile (to HIVE)
load data local inpath 'log.csv' into table logs;

CREATE TABLE logs_states(
    state string,
    name string,
    minLat float,
    maxLat float,
    minLon float,
    maxLon float)
    row format delimited
    fields terminated by ','
    TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data into table from localfile (to HIVE)
load data local inpath 'usa_states.csv' into table logs_states;

SELECT stateName as State, Quadrant, count(*) as Num_Monitors
FROM (      SELECT stateName, countyCode, siteNum,
            CASE
                WHEN AVG(lat) > AVG((minLat+maxLat)/2) AND AVG(lon) > AVG((minLon+maxLon)/2) THEN 'NE' 
                WHEN AVG(lat) > AVG((minLat+maxLat)/2) AND AVG(lon) < AVG((minLon+maxLon)/2) THEN 'SE' 
                WHEN AVG(lat) < AVG((minLat+maxLat)/2) AND AVG(lon) < AVG((minLon+maxLon)/2) THEN 'SW' 
                WHEN AVG(lat) < AVG((minLat+maxLat)/2) AND AVG(lon) > AVG((minLon+maxLon)/2) THEN 'NW' 
                ELSE 'Center or Borders' 
            END AS Quadrant 
            FROM (SELECT stateName, countyCode, siteNum, latitude AS lat, longitude AS lon, minLat, maxLat, minLon, maxLon FROM logs JOIN logs_states ON stateName = name ) table1
            GROUP BY stateName, countyCode, siteNum ) table2
GROUP BY stateName, Quadrant
