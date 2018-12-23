#!/usr/bin/env python
import sys
import string

for line in sys.stdin:
    line = line.strip()
    #Split line into array of entry data
    entries = line.split(",")
    print('{0:s} \t  {1:d}'.format(entries[2], 1))