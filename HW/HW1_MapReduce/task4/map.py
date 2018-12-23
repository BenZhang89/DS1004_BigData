#!/usr/bin/env python
import sys
import string
import csv
import io

for line in sys.stdin:
	line = line.strip()
	iofile = io.StringIO(line)
	reader = csv.reader(iofile)
	for i in reader:
		line = i
	if line[16] == "NY":
		print('{0:s}\t{1:d}'.format("NY", 1))
	else:
		print('{0:s}\t{1:d}'.format("Other", 1))
