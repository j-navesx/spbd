import sys

last_state_county_site_numb = None

for line in sys.stdin:
    
    line = line.strip()
    
    #line = line.translate(str.maketrans('', '', string.punctuation+'«»'))
    #print(line)
    state_county_site_numb,lat,lon = line.split('\t')
    #print(county_state_site_numb,lat,lon)
    #state,county,site_numb = state_county_site_numb.split(' ,') #space important because of the print done in the map function 
    #print(county,state,site_numb)                              #or else state != stat[1] always
    #decide_quadr(lat,lon,state)
    
    #if line == last_line:
        #break
        #total_count += count;
        #total_times += 1;

    if state_county_site_numb != last_state_county_site_numb:
        if last_state_county_site_numb:   #if None then doesn't print
            #average_pol = float(total_count/total_times)
            print(f"{line}")
        last_state_county_site_numb = state_county_site_numb
        
#if last_state_county_site_numb:     #parece-me desnecessário
    #average_pol = float(total_count/total_times)
    #print(f"LAST {line}")
    
#cat "epa_hap_daily_summary-small.csv" | python Trab_Final_MapReduce_Teste_exec3_Mapper1.py no terminal
