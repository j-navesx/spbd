import sys
sys.stdin.readline()   #reading the first line to remove the line with the column description
for line in sys.stdin:
    
    line = line.strip()
    
    #line = line.translate(str.maketrans('', '', string.punctuation+'«»'))
    
    words = line.split(',')
    
    print(words[24],',',words[25],',',words[11][:-6],'\t'+words[16])  # 16 is arithmetic mean value for the day, so we sum each ocurrence 
                                                              #to get total pollutant's value for each state and county then divide by nr of times 
                                                              #11 is local date_time
#cat "epa_hap_daily_summary-small.csv" | python Trab_Final_MapReduce_Teste_exec3_Mapper1.py no terminal
