import sys

MAGIC = 9999999

for line in sys.stdin:
    line = line.strip()
    code, name, value = line.split('\t')
    value = float(value)
    print("{0:.5f}\t{1}\t{2}".format(MAGIC-value, code, name))