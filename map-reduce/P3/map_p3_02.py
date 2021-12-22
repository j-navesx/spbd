import sys

MAGIC = 9999999

for line in sys.stdin:
    
    line = line.strip()
    
    county_name,year,avg_time = line.split(',')

    print(f'{year},{(MAGIC-float(avg_time))},{county_name}')
