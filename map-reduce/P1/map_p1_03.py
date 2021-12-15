from sys import stdin
import logging

MAGIC = 999999999

def main():
    stdin.readline()
    for line in stdin:
        line = line.strip()
        logging.debug(line)
        key, value = line.split(':')
        print(f"{MAGIC - int(value)}:{key}")


if __name__ == '__main__':
    logging.basicConfig(filename='./map-reduce/debug.log', level=logging.DEBUG)
    main()
