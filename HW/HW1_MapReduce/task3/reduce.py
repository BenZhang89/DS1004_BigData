#!/usr/bin/env python

import sys

current_type = None

for line in sys.stdin:   
    # remove leading and trailing whitespace
	line = line.strip()      
    
	Type, amount = line.split('\t',1)     
#	try:
	amount = float(amount)
#	except ValueError:
#		continue
        
	if current_type == Type:
		current_amount += amount
		current_count += 1


        
	else:
		if current_type:
            # write result to STDOUT
			print('{0:s} \t {1:.2f}, {2:.2f}'.format(current_type, current_amount, current_amount/current_count))
		current_amount = amount
		current_count = 1
		current_type = Type
        
if current_type == Type:
	print('{0:s} \t {1:.2f}, {2:.2f}'.format(current_type, current_amount, current_amount/current_count))
