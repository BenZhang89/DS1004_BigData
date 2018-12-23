#!/usr/bin/env python

import sys

current_vehicle = None
max_vehicle = None
max_count = 0
current_count = 0

for line in sys.stdin:   
    # remove leading and trailing whitespace
    line = line.strip()      
    vehicle, count = line.split('\t',1)  

    try:
        count = int(count)
    except ValueError:
        continue
        
    if current_vehicle == vehicle:
        current_count += count

      
    else:
        if current_vehicle:
            if current_count > max_count:
                max_count = current_count
                max_vehicle = current_vehicle
                
        current_vehicle = vehicle
        current_count = count
        

print('{0:s} \t {1:d}'.format(max_vehicle, max_count))
    
