from sys import stdin

def main():
    line = None
    last_county = None
    last_site = None
    for line in stdin:
        line = line.strip()
        county, site, state, lat, lon = line.split(',')
        if county != last_county or site != last_site:
            print(f"{line}")
            last_county = county
            last_site = site
    print(f"{line}")


if __name__ == '__main__':
    main()