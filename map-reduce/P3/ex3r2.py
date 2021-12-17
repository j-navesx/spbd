import sys
#sys.stdin.readline()   #reading the first line to remove the line with the column description
for line in sys.stdin:
    
    #line = line.strip()
    
    #line = line.translate(str.maketrans('', '', string.punctuation+'«»'))
    
    avg_time,county_state_date= line.split('\t')
    
    
    print(avg_time,county_state_date)
    #print(avg_time,'\t',county_state_date) #avg_time,'\t',
    
#cat "epa_hap_daily_summary-small.csv" | python Trab_Final_MapReduce_Teste_exec3_Mapper1.py no terminal
