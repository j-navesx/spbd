from sys import stdin

def main():
    stdin.readline()
    for line in stdin:
        line = line.strip()
        line = line.split(',')
        # StateCode+CountyCode,Site,State,Latitude,Longitude
        line[5] = float("{:.3f}".format(float(line[5])))
        line[6] = float("{:.3f}".format(float(line[6])))
        print(f"{line[0]+line[1]},{line[2]},{line[24]},{line[5]},{line[6]}")


if __name__ == '__main__':
    main()
