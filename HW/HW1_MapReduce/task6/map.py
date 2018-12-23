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
	print('{0:s},{1:s}\t{2:d}'.format(line[14],line[16],1))
