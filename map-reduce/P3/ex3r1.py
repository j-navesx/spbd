
import sys

total_times = 0
total_count = 0
average_pol = 0
lastKey = None

for line in sys.stdin:
    
    key,count = line.split('\t')
    
    count = float(count)
    
    #print(key,count)
    
    if key == lastKey:
        total_count += count;
        total_times += 1;

    else:
        if lastKey:
            average_pol = float(total_count/total_times)
            print(f"{lastKey}\t{average_pol:.5f}")
        lastKey = key
        total_count = count
        total_times = 1;

if lastKey:
    average_pol = float(total_count/total_times)
    print(f"{lastKey}\t{average_pol:.5f}")
    

#!cat "epa_hap_daily_summary-small.csv" | python ./Trab_Final_MapReduce_exec3_Mapper1.py | sort -k1,1 | python ./Trab_Final_MapReduce_exec3_Reducer1.py
