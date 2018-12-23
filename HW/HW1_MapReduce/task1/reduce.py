#!/usr/bin/env python

import sys

current_key = None
unique = False
printentry = None
for line in sys.stdin:   
    # remove leading and trailing whitespace
    line = line.strip()
    key,value = line.split('\t',1)
    entry=value.split(",") 

    
    if key == current_key:   # for the assignment of current key, value
        unique = False

        
    else: 
        if current_key:    #print the previous key,value  
            if unique and printentry: 
                print('{0:s}\t{1:s}'.format(current_key, Printvalue)) 
            printentry = None
            
        if len(entry)>1:
            Printvalue = value
            unique = True
            printentry = True
        current_key = key 
            
        
if unique and printentry:
    print('{0:s} \t {1:s}'.format(current_key, Printvalue))

