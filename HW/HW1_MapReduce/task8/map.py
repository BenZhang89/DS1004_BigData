#!/usr/bin/env python

import sys
import csv


for line in csv.reader(sys.stdin):
	if line[20]=="":
		line[20] = "NONE"
	if line[19] =="":
		line[19] = "NONE"
	print('{0:s}?{1:s}\t{2:d}'.format('1M',line[20], 1))
	print('{0:s}?{1:s}\t{2:d}'.format('2C',line[19], 1))
