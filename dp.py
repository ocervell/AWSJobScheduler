#!/usr/bin/env python
import time
import sys
import os
import subprocess
import boto.sqs
from credentials import access_key_id
from credentials import secret_access_key

ADD_WORKER_CONST = 1
FNULL = open(os.devnull, 'w')

if __name__ == '__main__':
		last_size = 0
		test = 0 #testing
		main_q = None #Main queue
		
		#Connect to SQS and get main queue
		try:
			conn = boto.sqs.connect_to_region("us-west-2",aws_access_key_id = access_key_id,aws_secret_access_key = secret_access_key)
			main_q = conn.create_queue('Main')
		except Exception as e:
			print e
			print("[info] Connection to SQS failed. Exiting.")
			sys.exit(0)
		print("[info] Connection to SQS success.")
		while(True):
			try:
				current_size = main_q.count()
				print "Current size = %d" % current_size
				if((current_size - last_size) >= ADD_WORKER_CONST):
					subprocess.call("ec2-run-instances ami-23653113 -n 1 -t t2.micro -k jahmyst -g launch-wizard-1 --instance-initiated-shutdown-behavior terminate --user-data-file init.sh -O %s -W %s" %(access_key_id, secret_access_key),shell=True, stdout=FNULL, stderr=subprocess.STDOUT);
					print("[info] 1 worker added.")
					time.sleep(20)
				last_size = current_size
				time.sleep(1)
			except KeyboardInterrupt:
				sys.exit(0)
