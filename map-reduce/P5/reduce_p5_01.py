import sys

last_state = None
last_county = None
last_site = None
last_lat = None
last_lon = None

for line in sys.stdin:
    
    line = line.strip()
    
    state,countyCode,siteNum,lat,lon = line.split(',')

    if last_state != state or last_county != countyCode or last_site != siteNum or lat != last_lat or lon != last_lon:
        if last_state:
            print(f"{last_state},{last_county},{last_site},{last_lat},{last_lon}")
        last_state = state
        last_county = countyCode
        last_site = siteNum
        last_lat = lat
        last_lon = lon
