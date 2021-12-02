from sys import stdin
import logging

MAGIC = 999999999

def main():
    for rank,line in enumerate(stdin):
        line = line.strip()
        key, value = line.split(':')
        print(f"{rank+1}. {value}:{MAGIC - int(key)}")
        

if __name__ == '__main__':
    logging.basicConfig(filename='./map-reduce/debug.log', level=logging.DEBUG)
    main()