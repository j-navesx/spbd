drop database if exists project_db cascade ;
create database project_db ;
use project_db ;

CREATE TABLE log(
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
load data local inpath 'log.csv' into table log;

-- Select 
select countyCode as County_Code,
countyName as County_Name, 
avg(Pollutant_Mean) as Pollutant_Levels
from ( select countyCode, countyName, arithmeticMean as Pollutant_Mean from weblog) table1
group by countyCode, countyName;
