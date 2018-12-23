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
	if len(line) == 22:
		print('{0:s} \t {1:s}, {2:s}, {3:s}, {4:s}'.format(line[0], line[14], line[6], line[2], line[1]))
	if len(line) == 18:
		print('{0:s} \t {1:s}'.format(line[0], "Open_violations"))

