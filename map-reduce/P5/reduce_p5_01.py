import sys

last_state_county_site_numb = None

for line in sys.stdin:
    
    line = line.strip()
    
    state_county_site_numb,lat,lon = line.split('\t')

    if state_county_site_numb != last_state_county_site_numb:
        if last_state_county_site_numb:   #if None then doesn't print
            print(f"{line}")
        last_state_county_site_numb = state_county_site_numb