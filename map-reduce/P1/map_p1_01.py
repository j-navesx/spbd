from sys import stdin
import logging

def main():
    stdin.readline()
    for line in stdin:
        line = line.strip()
        print(f"{line.split(',')[24]}:1")


if __name__ == '__main__':
    logging.basicConfig(filename='./map-reduce/debug.log', level=logging.DEBUG)
    main()
