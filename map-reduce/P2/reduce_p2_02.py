import sys

for line in sys.stdin:
    line = line.strip()
    value, code, name = line.split('\t')
    print("%s\t%s\t%s" % (code, name, value))