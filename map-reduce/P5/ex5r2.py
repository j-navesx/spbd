import sys

states = list()
last_state = None
NW_count = 0
NE_count = 0
SW_count = 0
SE_count = 0

with open("usa_states.csv", encoding = 'utf-8') as states_file:
        states = states_file.read().split('\n') #create a list of lists and each list has a row
        states = [l.split(',') for l in states]
        states.pop(0)
        states.pop(-1)
        for state in states:
            midlat = ((float(state[3])-float(state[2]))/2.0)+float(state[2])  #2 is min_lat   3 is max_lat
            midlon = (abs(float(state[5])-float(state[4]))/2.0)+float(state[4])  #4 is min_lon   5 is max_lon
            state.append(midlat) #state[6]
            state.append(midlon) #state[7]
    
#print(states)

def decide_quadr(latitude, longitude, state):
    for stat in states:
    #print(stat[1],state)
        if stat[1] == state:
            #print("LINE")
            #print(state_county_site_numb,lat,lon) #state line
            #print("LIST")
            #print(stat[1],stat[6],stat[7])        
            if (float(lat) < float(stat[6]) and float(lon) < float(stat[7])):  #states_list 6 -> mid_lat  and   7 -> mid_lon
                #print("NW")
                global NW_count
                NW_count += 1
            elif (float(lat) > float(stat[6]) and float(lon) < float(stat[7])):
                #print("NE")
                global NE_count
                NE_count += 1
            elif (float(lat) < float(stat[6]) and float(lon) > float(stat[7])):
                #print("SW")
                global SW_count
                SW_count += 1
            elif (float(lat) > float(stat[6]) and float(lon) > float(stat[7])):
                #print("SE")
                global SE_count
                SE_count += 1
            #break
        #print(stat[1])
        #print(stat[1],state,lat,lon)

    
#sys.stdin.readline()   #reading the first line to remove the line with the column description
for line in sys.stdin:
    
    line = line.strip()
    
    #line = line.translate(str.maketrans('', '', string.punctuation+'«»'))
    #print(line)
    state_county_site_numb,lat,lon = line.split('\t')
    #print(county_state_site_numb,lat,lon)
    state,county,site_numb = state_county_site_numb.split(' ,') #space important because of the print done in the map function 
    #print(county,state,site_numb)                              #or else state != stat[1] always
    #decide_quadr(lat,lon,state)
    
    if state == last_state:        
        decide_quadr(lat,lon,state)    
    else:
        if last_state:
            #average_pol = float(total_count/total_times)
            print(f"State: {last_state}\t NW: {NW_count}\t NE: {NE_count}\t SW: {SW_count}\t SE: {SE_count}")
        last_state = state
        NW_count = 0
        NE_count = 0
        SW_count = 0
        SE_count = 0
        decide_quadr(lat,lon,state)

if last_state:
    print(f"State: {last_state}\t NW: {NW_count}\t NE: {NE_count}\t SW: {SW_count}\t SE: {SE_count}")   

    
