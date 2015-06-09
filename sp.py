#!/usr/bin/env python
import time
import os
import subprocess
import boto.sqs
from credentials import access_key_id
from credentials import secret_access_key
import argparse

FNULL = open(os.devnull, 'w')

if __name__ == '__main__':
		main_q = None #Main queue
		parser = argparse.ArgumentParser()
		parser.add_argument('-n',
							'--number',
							type = int,
							default = 1,
							metavar='<NUM>')
		parser.add_argument('-t',
							'--itype',
							default = 't2.micro',
							metavar='<INSTANCE_TYPE>')
		args = parser.parse_args()
		print("\n=======================")
		print("Instance type : %s" %args.itype)
		print("Number of instances: %d" %args.number)
		print("=======================")

		#Connect to SQS and get main queue
		try:
			conn = boto.sqs.connect_to_region("us-west-2",aws_access_key_id = access_key_id,aws_secret_access_key = secret_access_key)
			main_q = conn.get_queue('Main')
		except:
			print("[info] Connection to SQS failed. Exiting.")
			sys.exit(0)
		print("[info] Connection to SQS success.")
		try:
			subprocess.call("ec2-run-instances ami-23653113 -n %d -t %s -k jahmyst -g launch-wizard-1 --instance-initiated-shutdown-behavior terminate --user-data-file init.sh -O %s -W %s" %(args.number, args.itype, access_key_id, secret_access_key),shell=True, stdout=FNULL, stderr=subprocess.STDOUT);
		except:
			print("[info] Error launching instances. Exiting.")
		print("[info] %d %s instances were launched successfully" %(args.number, args.itype))
