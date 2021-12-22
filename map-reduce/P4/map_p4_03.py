from sys import stdin

def main():
    for line in stdin:
        line = line.strip()
        state, distance = line.split(',')
        # State,Distance
        print(f"{state},{distance}")


if __name__ == '__main__':
    main()
