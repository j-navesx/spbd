import sys
#sys.stdin.readline()   #reading the first line to remove the line with the column description
for line in sys.stdin:
    
    line = line.strip()
    
    #line = line.translate(str.maketrans('', '', string.punctuation+'«»'))
    
    county_state_date,avg_time = line.split('\t')
    
    #float(avg_time)
    #print(line)
    print(f'{avg_time:>010}\t{county_state_date}')
    
#cat "epa_hap_daily_summary-small.csv" | python Trab_Final_MapReduce_Teste_exec3_Mapper1.py no terminal
