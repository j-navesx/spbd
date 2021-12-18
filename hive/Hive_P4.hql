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
load data local inpath 'epa_hap_daily_summary-small.csv' into table logs;

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

SELECT stateName as State, avg(Dist_Monitor_Center) as Avg_Dist_Monitor_Center
FROM (      SELECT stateName, countyCode, siteNum, sqrt( pow( (lat-((minLat+maxLat)/2))*111, 2) + pow( (lon-((minLon+maxLon)/2))*111, 2) ) as Dist_Monitor_Center
            FROM (SELECT stateName, countyCode, siteNum, latitude AS lat, longitude AS lon, minLat, maxLat, minLon, maxLon FROM logs JOIN logs_states ON stateName = name ) table1
            GROUP BY stateName, countyCode, siteNum, lat, lon, minLat, maxLat, minLon, maxLon ) table2
GROUP BY stateName
ORDER BY Avg_Dist_Monitor_Center DESC