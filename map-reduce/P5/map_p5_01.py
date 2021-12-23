import sys
sys.stdin.readline()   #reading the first line to remove the line with the column description


for line in sys.stdin:
    line = line.strip()
    line = line.split(',')
    
    line[5] = round(float(line[5]),3)
    line[6] = round(float(line[6]),3)
    
    print(f"{line[24]},{line[1]},{line[2]}\t{line[5]}\t{line[6]}")
    #print(line[24],',',line[1],',',line[2],'\t'+line[5],'\t',line[6])  #1 is countyCode 2 is site numb, 5 is lat, 6 is lon
