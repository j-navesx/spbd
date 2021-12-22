from sys import stdin

def main():
    last_state = None
    distance_sum = 0
    distance_len = 0
    for line in stdin:
        line = line.strip()
        state, distance = line.split(',')
        if state != last_state:
            if last_state:
                print(f"{last_state}: {distance_sum/distance_len}")
            last_state = state
            distance_sum = 0
            distance_len = 1
        else:
            distance_sum += float(distance)
            distance_len += 1


if __name__ == '__main__':
    main()