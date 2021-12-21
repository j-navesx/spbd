import sys
sys.stdin.readline()   #reading the first line to remove the line with the column description


for line in sys.stdin:
    
    line = line.strip()
    
    words = line.split(',')
    
    print(words[24],',',words[1],',',words[2],'\t'+words[5],'\t',words[6])  #1 is countyCode 2 is site numb, 5 is lat, 6 is lon
    
    
#cat "epa_hap_daily_summary-small.csv" | python Trab_Final_MapReduce_Teste_exec3_Mapper1.py no terminal
