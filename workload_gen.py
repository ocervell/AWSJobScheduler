#!/usr/bin/env python
# workload_gen.py
import string
import random
import sys

def generate_workload_file(nb_tasks, SLEEP):
	with open('custom.workload','w') as workload_file:
		for i in range(nb_tasks):
			workload_file.write('sleep ' + SLEEP  +'\n')

if __name__ =="__main__":
	if len(sys.argv) != 3:
		print "Usage: workload_gen <number of sleep tasks> <size of sleep (milliseconds)>"
		sys.exit(1)
	
	nb_tasks = int(sys.argv[1])
	SLEEP = sys.argv[2]
	generate_workload_file(nb_tasks, SLEEP)


