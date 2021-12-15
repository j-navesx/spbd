from sys import stdin
import logging

last_key = None

def main():
    stdin.readline()
    for line in stdin:
        line = line.strip()
        key, value = line.split(':')
        if key != last_key:
            print(f"{key}:{value}")
            last_key = key


if __name__ == '__main__':
    logging.basicConfig(filename='./map-reduce/debug.log', level=logging.DEBUG)
    main()