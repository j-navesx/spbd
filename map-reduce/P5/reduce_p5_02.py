import sys

STATE_FILE = "../data/usa_states.csv"
last_state = None
NW_count = 0
NE_count = 0
SW_count = 0
SE_count = 0

with open(STATE_FILE, encoding="utf-8") as states_file:
        states = states_file.read().split('\n') #create a list of lists and each list has a row
        states.pop(0)
        states.pop(-1)
        states = {l.split(',')[1]:(\
            ((float(l.split(',')[3])-float(l.split(',')[2]))/2.0)+float(l.split(',')[2]),\
            (abs(float(l.split(',')[5])-float(l.split(',')[4]))/2.0)+float(l.split(',')[4])\
            ) for l in states}
        #mid_lat, mid_lon  
    
for line in sys.stdin:
    
    line = line.strip()
    
    state_county_site_numb,lat,lon = line.split('\t')

    state,county,site_numb = state_county_site_numb.split(',') #space important because of the print done in the map function 
                                                              #or else state != stat[1] always

    if state == last_state:        
       
        if last_state in states:
            mid_lat, mid_lon = states[state]

            if (float(lat) < float(mid_lat) and float(lon) < float(mid_lon)):  #states_list 6 -> mid_lat  and   7 -> mid_lon

                NW_count += 1
            elif (float(lat) < float(mid_lat) and float(lon) > float(mid_lon)):

                NE_count += 1
            elif (float(lat) > float(mid_lat) and float(lon) < float(mid_lon)):

                SW_count += 1
            elif (float(lat) > float(mid_lat) and float(lon) > float(mid_lon)):

                SE_count += 1
    else:
        if last_state:
            if last_state in states:
                print(f"State: {last_state}\t NW: {NW_count}\t NE: {NE_count}\t SW: {SW_count}\t SE: {SE_count}")
        last_state = state
        NW_count = 0
        NE_count = 0
        SW_count = 0
        SE_count = 0
        
        if last_state in states:
            mid_lat, mid_lon = states[state]

            if (float(lat) < float(mid_lat) and float(lon) < float(mid_lon)):  #states_list 6 -> mid_lat  and   7 -> mid_lon
                NW_count += 1
            
            elif (float(lat) < float(mid_lat) and float(lon) > float(mid_lon)):
                NE_count += 1
            
            elif (float(lat) > float(mid_lat) and float(lon) < float(mid_lon)):
                SW_count += 1
            
            elif (float(lat) > float(mid_lat) and float(lon) > float(mid_lon)):
                SE_count += 1