#!/usr/bin/env python

import sys

current_vehicle = None
current_count = 0
top = {}
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
            if len(top) == 20:
                keymin=min(top, key=top.get)
                if top[keymin] < current_count:
                    top.pop(keymin)
                    top[current_vehicle] = current_count
            else:
                top[current_vehicle] = current_count
                
        current_vehicle = vehicle
        current_count = count
        
for k,v in dict(sorted(top.items(), key=lambda x: x[1],reverse=True)).items():
    print('{0:s} \t {1:d}'.format(k, v))
    
