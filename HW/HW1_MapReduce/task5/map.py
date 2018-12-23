#!/usr/bin/env python
import sys
import string

for line in sys.stdin:
    line = line.strip()
    #Split line into array of entry data
    entries = line.split(",")
    print('{0:s},{1:s}\t{2:d}'.format(entries[14],entries[16],1))