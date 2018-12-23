#!/usr/bin/env python
import sys
import csv

current_term = None
current_count = 0
for line in sys.stdin:   
	line = line.strip()      
	term, count = line.split('\t') 
	term = term.split("?")
    
	count = int(count)
        
	if current_term == term:
		current_count += count
        
	else:
		if current_term:
			if current_term[0]=='1M':
				print('{0:s} \t {1:s},{2:d}'.format('vehicle_make', current_term[1], current_count))
			else:
				print('{0:s} \t {1:s},{2:d}'.format('vehicle_color', current_term[1], current_count))
		current_term = term
		current_count = count
        
if current_term[0] == '1M':
	print('{0:s} \t {1:s},{2:d}'.format('vehicle_make',current_term[1], current_count))
else:
	print('{0:s} \t {1:s},{2:d}'.format('vehicle_color', current_term[1], current_count))
