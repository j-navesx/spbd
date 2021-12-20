from sys import stdin
import logging

def main():
    for line in stdin:
        line = line.strip()
        logging.debug(line)
        _key, value = line.split(':')
        print(f"{value}:1")

if __name__ == '__main__':
    main()