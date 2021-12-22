import sys

total_times = 0
total_count = 0
average_pol = 0
lastKey = None

for line in sys.stdin:
    
    key = line.split(',')[0]+','+line.split(',')[1]
    count = line.split(',')[2]
    
    count = float(count)
        
    if key == lastKey:
        total_count += count;
        total_times += 1;

    else:
        if lastKey:
            average_pol = float(total_count/total_times)
            print(f"{lastKey},{average_pol:.5f}")
        lastKey = key
        total_count = count
        total_times = 1;

if lastKey:
    average_pol = float(total_count/total_times)
    print(f"{lastKey},{average_pol:.5f}")
    