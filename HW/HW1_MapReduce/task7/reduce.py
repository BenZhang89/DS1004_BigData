#!/usr/bin/env python

import sys

current_code = None
weekends = ['05', '06', '12', '13', '19', '20', '26', '27']
firstline =  True
for line in sys.stdin:   
    # remove leading and trailing whitespace
	line = line.strip()      
	code, date = line.split('\t',1) 
	date = date.split('-')
        
	if current_code == code:
		if date[2] in weekends:
			weekend_count += 1
		else:
			weekday_count += 1 
      
	else:
		if current_code:
			print('{0:s} \t {1:.2f},{2:.2f}'.format(current_code, (weekend_count/8.0), (weekday_count/23.0)))
			if date[2] in weekends:
				weekend_count = 1
				weekday_count = 0
			else:
				weekend_count = 0
				weekday_count = 1
		current_code = code
		if firstline:
			if date[2] in weekends:
				weekend_count = 1
				weekday_count = 0
			else:
				weekend_count = 0
				weekday_count = 1
		firstline = False
if current_code == code:
	print('{0:s} \t {1:.2f},{2:.2f}'.format(current_code, (weekend_count/8.0), (weekday_count/23.0)))
