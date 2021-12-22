import sys

MAGIC = 9999999

for line in sys.stdin:
    
    line = line.strip()
      
    year,avg,county = line.split(',')
     
    print(year,county,MAGIC-float(avg))
    
