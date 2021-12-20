from sys import stdin
import logging

last_county = None
last_site = None

def main():
    for line in stdin:
        line = line.strip()
        county, site, state, lat, lon = line.split(',')
        if county != last_county or site != last_site:
            print(f"{line}")
            last_county = county
            last_site = site
            


if __name__ == '__main__':
    main()