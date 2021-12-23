SELECT stateName as State, 
Quadrant, 
count(*) as Num_Monitors
FROM (
    SELECT stateName, 
    countyCode, 
    siteNum,
    CASE
        WHEN AVG(lat) > AVG((minLat+maxLat)/2) AND AVG(lon) > AVG((minLon+maxLon)/2) THEN 'NE' 
        WHEN AVG(lat) > AVG((minLat+maxLat)/2) AND AVG(lon) < AVG((minLon+maxLon)/2) THEN 'SE' 
        WHEN AVG(lat) < AVG((minLat+maxLat)/2) AND AVG(lon) < AVG((minLon+maxLon)/2) THEN 'SW' 
        WHEN AVG(lat) < AVG((minLat+maxLat)/2) AND AVG(lon) > AVG((minLon+maxLon)/2) THEN 'NW' 
        ELSE 'Center or Borders' 
    END AS Quadrant 
    FROM (
        SELECT stateName, 
        countyCode, 
        siteNum, 
        latitude AS lat, 
        longitude AS lon, 
        minLat, 
        maxLat, 
        minLon, 
        maxLon 
        FROM logs 
        JOIN logs_states ON stateName = name 
    ) table1
    GROUP BY stateName, countyCode, siteNum 
) table2
GROUP BY stateName, Quadrant