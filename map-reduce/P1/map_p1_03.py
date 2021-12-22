from sys import stdin
import logging



def main():
    MAGIC = 999999999
    
    for line in stdin:
        line = line.strip()
        logging.debug(line)
        key, value = line.split(':')
        print(f"{MAGIC - int(value)}:{key}")


if __name__ == '__main__':
    main()
