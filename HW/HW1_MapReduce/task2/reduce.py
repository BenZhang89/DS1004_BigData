#!/usr/bin/env python

import sys

current_code = None
current_count = 0

for line in sys.stdin:   
    # remove leading and trailing whitespace
    line = line.strip()
    code, count = line.split('\t',1)
    
    try:
        count = int(count)
    except ValueError:
        continue   
        
    if current_code == code:
        current_count += count
        
    else:
        if current_code:
            # write result to STDOUT
            print('{0:s} \t {1:d}'.format(current_code, current_count))
        current_count = count
        current_code = code
            
if current_code == code:
    print('{0:s} \t {1:d}'.format(current_code, current_count))
