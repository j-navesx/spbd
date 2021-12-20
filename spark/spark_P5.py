#!/usr/bin/env python
# coding: utf-8

# In[117]:


#!/usr/bin/env python
import pyspark

totalExecTime = 0
sc = pyspark.SparkContext('local[*]')
try :
    lines = sc.textFile('epa_hap_daily_summary-small.csv')
    usa_states_lines = sc.textFile('usa_states.csv')

    logTuples = lines.filter( lambda line: len(line) > 0)                      .zipWithIndex()                      .filter( lambda x: x[1] > 0)                      .map(lambda x: x[0])                      .map( lambda line: line.split(','))                      .map( lambda arr: (arr[24], (float(arr[5]),float(arr[6])) ))                      .distinct()\ #ignore repeated entries with the same state, lat and lon
                     .sortByKey()   
                     #.map( lambda arr: ((arr[11][:4], arr[24]), float(arr[16])))
    
    statesTuples = usa_states_lines.filter( lambda line: len(line) > 0)                      .zipWithIndex()                      .filter( lambda x: x[1] > 0)                      .map(lambda x: x[0])                      .map( lambda line: line.split(','))                      .map( lambda arr: (arr[1], [float(arr[2]),float(arr[3]),float(arr[4]),float(arr[5])] ))                     .mapValues(lambda x: ((x[0]+x[1])/2,(x[2]+x[3])/2))  #calculate midlat and midlon
    

    #for (key,value) in statesTuples.take(20):
        #print(key,value)
    
    #print('----------------')
    
    tuplesJoined = logTuples.join(statesTuples).mapValues(lambda x: [x[0][0],x[1][0],x[0][1],x[1][1]]) 
    
    stateCoords1stQuad = tuplesJoined.filter(lambda x: float(x[1][0]) < float(x[1][1]) and float(x[1][2]) < float(x[1][3]))                                 
    stateCoords2ndQuad = tuplesJoined.filter(lambda x: float(x[1][0]) > float(x[1][1]) and float(x[1][2]) < float(x[1][3]))
    stateCoords3rdQuad = tuplesJoined.filter(lambda x: float(x[1][0]) < float(x[1][1]) and float(x[1][2]) > float(x[1][3]))
    stateCoords4thQuad = tuplesJoined.filter(lambda x: float(x[1][0]) > float(x[1][1]) and float(x[1][2]) > float(x[1][3]))
    
    stateMonitors1stQuad = stateCoords1stQuad.mapValues(lambda x: (1))                                              .reduceByKey(lambda a,b: a+b)                                             .mapValues(lambda x: ('NW',x))
    
    stateMonitors2ndQuad = stateCoords2ndQuad.mapValues(lambda x: (1))                                              .reduceByKey(lambda a,b: a+b)                                             .mapValues(lambda x: ('NE',x))
    
    stateMonitors3rdQuad = stateCoords3rdQuad.mapValues(lambda x: (1))                                              .reduceByKey(lambda a,b: a+b)                                             .mapValues(lambda x: ('SW',x))
    
    stateMonitors4thQuad = stateCoords4thQuad.mapValues(lambda x: (1))                                              .reduceByKey(lambda a,b: a+b)                                             .mapValues(lambda x: ('SE',x))
    
    finalStateMonitorsPerQuad = stateMonitors1stQuad.join(stateMonitors2ndQuad)                                                    .join(stateMonitors3rdQuad)                                                    .join(stateMonitors4thQuad)                                                    .sortByKey()
    
    for (key,value) in finalStateMonitorsPerQuad.collect():
        print(key,value)
    
    #print('----------------')
    
    #for (key,value) in tuplesJoined.take(40):
        #print(key,value)
    
    #for (minlat,maxlat, minlon, maxlon) in statesTuples.take(10):
        #print(minlat,maxlat, minlon, maxlon)   
    
    #for (key,value) in statesTuples.take(20):
        #print(key,value)
    
    #print('----------------')
    
    #for (key,value) in logTuples.take(10):
        #print(key,value)

    #print('----------------')
        
    #for (key,value) in statesTuples.take(20):
        #print(key,value)
    
    #print('-----------------')
    
    #tuplesJoined = logTuples.join(statesTuples).sortByKey()
    
    #for (key,value) in tuplesJoined.take(20):
        #print(key,value)
    
    #state_county_site_lat = logTuples.map( lambda arr: ((arr[24],arr[25],arr[2]), arr[5]))
    
    #state_county_site_lon = logTuples.map( lambda arr: ((arr[24],arr[25],arr[2]), arr[6]))
    
    #for (key,value) in state_county_site_lat.take(10):
        #print(key,value)
        
    #for (key,value) in state_county_site_lon.take(10):
        #print(key,value)
    
    sc.stop()
except Exception as e:
    print(e)
    sc.stop()


# In[ ]:




