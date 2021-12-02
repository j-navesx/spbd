import sys

currentCode = None
currentName = None
meanSum = 0.0
totalNR = 0

for line in sys.stdin:
    line = line.strip()
    countyCode, mean, name = line.split('\t')
    mean = float(mean)
    if countyCode == currentCode:
        meanSum += mean
        totalNR += 1
    else:
        if currentCode:
            print("%s\t%s\t%s" % (currentCode, currentName, meanSum/totalNR))
        currentCode = countyCode
        currentName = name
        meanSum = mean
        totalNR = 1
if currentCode:
    print("%s\t%s\t%s" % (currentCode, currentName, meanSum/totalNR))

            