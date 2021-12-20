from sys import stdin
import logging

def main():
    for line in stdin:
        line = line.strip()
        county, site, state, lat, lon = line.split(',')
        # State,Latitude,Longitude
        print(f"{state},{lat},{lon}")


if __name__ == '__main__':
    main()
