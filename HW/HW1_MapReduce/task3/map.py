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

#	entries = line.split(",")
#	if entries[2] == '':
#		continue
#	if entries[12] == '':
#		continue
	print('{0:s} \t {1:s}'.format(line[2], line[12]))
