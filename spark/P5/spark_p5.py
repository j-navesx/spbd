import pyspark

totalExecTime = 0
sc = pyspark.SparkContext('local[*]')
try :
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv')
    usa_states_lines = sc.textFile('../data/usa_states.csv')

    logTuples = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: ((arr[24], arr[1], arr[2]), (float("{:.3f}".format(float(arr[5]))),float("{:.3f}".format(float(arr[6])))) )) \
                     .distinct() \
                     .map(lambda arr: (arr[0][0],arr[1]))   
                     
    
    statesTuples = usa_states_lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: (arr[1], [float(arr[2]),float(arr[3]),float(arr[4]),float(arr[5])] ))\
                     .mapValues(lambda x: ((x[0]+x[1])/2,(x[2]+x[3])/2))  #calculate midlat and midlon
    
    tuplesJoined = logTuples.join(statesTuples).mapValues(lambda x: [x[0][0],x[1][0],x[0][1],x[1][1]]) 
    
    stateCoords1stQuad = tuplesJoined.filter(lambda x: float(x[1][0]) < float(x[1][1]) and float(x[1][2]) < float(x[1][3]))                                 
    stateCoords2ndQuad = tuplesJoined.filter(lambda x: float(x[1][0]) < float(x[1][1]) and float(x[1][2]) > float(x[1][3]))
    stateCoords3rdQuad = tuplesJoined.filter(lambda x: float(x[1][0]) > float(x[1][1]) and float(x[1][2]) < float(x[1][3]))
    stateCoords4thQuad = tuplesJoined.filter(lambda x: float(x[1][0]) > float(x[1][1]) and float(x[1][2]) > float(x[1][3]))
    
    stateMonitors1stQuad = stateCoords1stQuad.mapValues(lambda x: (1)) \
                                             .reduceByKey(lambda a,b: a+b)\
                                             .mapValues(lambda x: ('NW',x))
                                             
    
    stateMonitors2ndQuad = stateCoords2ndQuad.mapValues(lambda x: (1)) \
                                             .reduceByKey(lambda a,b: a+b)\
                                             .mapValues(lambda x: ('NE',x))
    
    stateMonitors3rdQuad = stateCoords3rdQuad.mapValues(lambda x: (1)) \
                                             .reduceByKey(lambda a,b: a+b)\
                                             .mapValues(lambda x: ('SW',x))
    
    stateMonitors4thQuad = stateCoords4thQuad.mapValues(lambda x: (1)) \
                                             .reduceByKey(lambda a,b: a+b)\
                                             .mapValues(lambda x: ('SE',x))
    
    #join won't do because some states dont #have any monitors in certain quadrants #like Florida (NW)
    #fullOuterJoin works, setting the quadrant's value as None if there are no monitors in that same quadrant
    
    finalStateMonitorsPerQuad = stateMonitors1stQuad.join(stateMonitors2ndQuad)\
                                                    .join(stateMonitors3rdQuad)\
                                                    .join(stateMonitors4thQuad)

    for key,value in finalStateMonitorsPerQuad.collect():
        print(f"{key}\t{value[0][0][0]} {value[0][0][1]} {value[0][1]} {value[1]}")
    
    sc.stop()
except Exception as e:
    print(e)
    sc.stop()