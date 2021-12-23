from sys import stdin
from math import sqrt

def main():
    STATE_FILE = "../data/usa_states.csv"

    with open(STATE_FILE, encoding="utf-8") as states_file:
        states = states_file.read().split('\n') #create a list of lists and each list has a row
        states.pop(0)
        states.pop(-1)
        states = {l.split(',')[1]:(\
            ((float(l.split(',')[3])-float(l.split(',')[2]))/2.0)+float(l.split(',')[2]),\
            (abs(float(l.split(',')[5])-float(l.split(',')[4]))/2.0)+float(l.split(',')[4])\
            ) for l in states}
        #mid_lat, mid_lon

    for line in stdin:
        line = line.strip()
        state, lat, lon = line.split(',')
        lat = float("{:.3f}".format(float(lat)))
        lon = float("{:.3f}".format(float(lon)))
        # Get the middle latitude and longitude
        if state in states:
            mid_lat, mid_lon = states[state]
            # Calculate distance to the center of the state
            distance = sqrt( pow((lat-mid_lat)*111,2) + pow((lon-mid_lon)*111,2) )
            print(f"{state},{distance}")
            

if __name__ == '__main__':
    main()