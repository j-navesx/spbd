from sys import stdin
import logging

def main():
    stdin.readline()
    for line in stdin:
        line = line.strip()
        line = line.split(',')
        # Monitor_id:County_Code:State_Name
        print(f"{line[2]}:{line[0]+line[1]}:{line[24]}")


if __name__ == '__main__':
    logging.basicConfig(filename='./map-reduce/debug.log', level=logging.DEBUG)
    main()
