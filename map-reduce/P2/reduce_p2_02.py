import sys

rank = 1
MAGIC = 9999999

for line in sys.stdin:
    line = line.strip()
    value, code, name = line.split('\t')
    print("%s. %s\t%s" % (rank, name, MAGIC-float(value)))
    rank += 1