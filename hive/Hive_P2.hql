-- Select the pollutant levels and orders based on ascending pollutant levels
select stateCode as State_Code,
countyCode as County_Code,
countyName as County_Name,
avg(Pollutant_Mean) as Pollutant_Levels
from ( 
    select stateCode, 
    countyCode, 
    countyName, 
    arithmeticMean as Pollutant_Mean 
    from logs
) table1
group by countyCode, stateCode, countyName
order by Pollutant_Levels;