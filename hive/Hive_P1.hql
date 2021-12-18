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

-- Select the pollutant levels and orders based on ascending pollutant levels
select stateName as State_Name,
countyName as County_Name,
size(set_monitors) as Nr_Monitors
from ( select stateName, countyName, collect_set(siteNum) as set_monitors from logs GROUP BY stateName, countyName) table1
ORDER BY Nr_Monitors DESC;