import sys

last_state_county_site_numb = None
last_lat = None
last_lon = None

for line in sys.stdin:
    
    line = line.strip()
    
    state_county_site_numb,lat,lon = line.split('\t')

    if state_county_site_numb != last_state_county_site_numb and lat != last_lat and lon != last_lon:
        if last_state_county_site_numb:   #if None then doesn't print
            print(f"{line}")
        last_state_county_site_numb = state_county_site_numb
        last_lat = lat
        last_lon = lon