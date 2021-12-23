-- Select the pollutant levels and orders based on ascending pollutant levels
select stateName as State_Name,
Year_date,
avg(Pollutant_Mean) as Pollutant_Levels
from ( 
    select stateName, 
    substring(dateLocal,1,4) as Year_date, 
    arithmeticMean as Pollutant_Mean 
    from logs
) table1
group by stateName, Year_date
order by Year_date, Pollutant_Levels desc;