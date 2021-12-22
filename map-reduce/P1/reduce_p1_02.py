from sys import stdin
import logging

def main():
    current_key = None
    accumulator = 1
    
    for line in stdin:
        line = line.strip()
        key, value = line.split(':')
        if key != current_key:
            if current_key: 
                print(f"{current_key}:{accumulator}")
            current_key = key
            accumulator = int(value)
        else:
            accumulator += int(value)
    print(f"{current_key}:{accumulator}")


if __name__ == '__main__':
    main()