SELECT stateName as State, 
avg(Dist_Monitor_Center) as Avg_Dist_Monitor_Center
FROM (
    SELECT stateName,
    sqrt( pow( (lat-((minLat+maxLat)/2))*111, 2) + pow( (lon-((minLon+maxLon)/2))*111, 2) ) as Dist_Monitor_Center
    FROM (
        SELECT stateName, 
        countyCode, 
        siteNum, 
        round(latitude,3) AS lat, 
        round(longitude,3) AS lon, 
        minLat, 
        maxLat, 
        minLon, 
        maxLon 
        FROM logs 
        JOIN logs_states ON stateName = name 
    ) table1
    GROUP BY stateName, 
    countyCode, 
    siteNum, 
    lat, 
    lon, 
    minLat, 
    maxLat, 
    minLon, 
    maxLon 
) table2
GROUP BY stateName
ORDER BY Avg_Dist_Monitor_Center DESC;