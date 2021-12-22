import sys

for line in sys.stdin:
    line = line.strip()
    code, name, value = line.split('\t')
    value = float(value)
    print("{0:.5f}\t{1}\t{2}".format(value, code, name))