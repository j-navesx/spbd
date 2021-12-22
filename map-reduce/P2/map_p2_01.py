
# Map-Reduce written in python to rank counties based on air quality
# The strategy for this will be to generate tuples of county codes(key) and arithmetic mean value of pollutant values
# We ignore the units(ppb and ug/m^3) and compare the values directly since we don't have the M(molecular weight) and temperature

import sys
file = sys.stdin
file.readline() # Skips the header line
for line in file:
    line = line.strip()
    words = line.split(',')
    print("%s\t%s\t%s" % ((words[0], words[1]), words[16], words[25]))
