from sys import stdin
import logging



def main():
    last_state = None
    distance_sum = 0
    distance_len = 0
    for line in stdin:
        line = line.strip()
        state, distance = line.split(',')
        if state != last_state:
            print(f"{state}: {distance_sum/distance_len}")
            last_state = state
        else:
            distance_sum += distance
            distance_len += 1
            


if __name__ == '__main__':
    main()