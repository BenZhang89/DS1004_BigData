#!/usr/bin/env python
import sys
import string

currentkey = None
is_valid=True
valid_entry=False
# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

        #Remove leading and trailing whitespace
        line = line.strip()

        #Get key/value 
        key, value = line.split('\t',1)

        #Parse key/value input (your code goes here)
        entry=value.split(",")

        #If we are still on the same key...
        if key==currentkey:

                #Process key/value pair (your code goes here)
                is_valid=False

        #Otherwise, if this is a new key...
        else:
                #If this is a new key and not the first key we've seen
                if currentkey:
                        if (is_valid and valid_entry):
                                print ('{0:s}\t{1:s},{2:s}, {3:s}, {4:s}'.format(currentkey, plate_id, violation_precinct, violation_code, issue_date))
                        valid_entry=False
                        
                if (len(entry)==4):
                        plate_id =entry[0]
                        violation_precinct=entry[1]
                        violation_code=entry[2]
                        issue_date=entry[3]
                        valid_entry=True
                        currentkey=key
                        is_valid=True
                
                        
                        #compute/output result to STDOUT (your code goes here)
if (currentkey!=key and valid_entry):
        print ('{0:s}\t{1:s},{2:s}, {3:s}, {4:s}'.format(currentkey, plate_id, violation_precinct, violation_code, issue_date))
