#!/usr/bin/env python

import sys
current_state = None
for line in sys.stdin:   
    # remove leading and trailing whitespace
	line = line.strip()      
	state, count = line.split('\t')
	count = int(count)
        
	if current_state == state:
		current_count += count
	else:
		if current_state:
			print('{0:s} \t {1:d}'.format(current_state, current_count))
		current_state = state
		current_count = count
print('{0:s} \t {1:d}'.format(current_state, current_count))
