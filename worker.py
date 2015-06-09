#!/usr/bin/env python
# server.py
import os
import subprocess
import sys 
import argparse
import json
import Queue
import boto.sqs
from boto.sqs.message import Message
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
from credentials import access_key_id
from credentials import secret_access_key
from threading import Thread
from time import sleep
from random import randint
from datetime import datetime

#Multi-level dictionary
class hash(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value 

################ GLOBAL VARIABLES ################
tasks = hash() #All tasks will be stored here
num_threads = 16
MAX_WORKERS = 16
conn = None #Connection to SQS
worker_q = None #Return SQS queue
DBtable = None #DynamoDB table
DBfields = None #DynamoDB fields
checkIdle_thread = None #Thread used to terminate instances
SQS_queues = [] #Workers queues
SQSWriters = [] #SQS Writers
SQSReaders = [] #SQS Readers
workers = [] #Workers that execute task
idle_variable = True
MAX_IDLE_TIME = 0
terminate = 0

class LocalWorker(Thread):
	def __init__(self):
		super(LocalWorker, self).__init__()
		self.running = True
		self.q = Queue.Queue()
		self.qlen = 0

	def add(self, data):
		self.q.put(data)
		self.qlen += 1
 
	def stop(self):
		self.running = False
 
	def run(self):
		q = self.q
		while self.running:
			try:
				# block for 1 second only:
				value = q.get(block=True, timeout=1)
				self.qlen -= 1
				clientid, jobid = process(value)
				do_sleep(clientid, jobid)
		
			except Queue.Empty:
				sys.stdout.flush()
		
		if not q.empty():
			print "Elements left in the queue:"
			while not q.empty():
				print q.get()


class SQSReader(Thread):
	def __init__(self, SQS):
		super(SQSReader, self).__init__()
		self.running = True
		self.SQS = SQS
		
	def stop(self):
		self.running = False
 
	def run(self):
		SQS = self.SQS
		while self.running:
			try:
				# block for 1 second only:
				q_length = SQS.count()	
				lmax = 200 #Max value to avoid client timeout
				l = 1
				while (q_length != 0):
					if (l == lmax):
						break
					m = SQS.read()
					value = json.loads(m.get_body())
					jobid = value[1]
					clientid = value[0].encode()
					if (checkDynamoDB(clientid, jobid) == True): #Checking for duplicate
						 print("[info] Duplicate for Job %d detected." %jobid)
						 break
					insertDynamoDB(clientid, jobid)
					i = find_min_queue(workers)
					workers[i].add(value)
					SQS.delete_message(m)
					print("[info] Job %d retrieved from SQS" %jobid)
					q_length -= 1
					l +=1
			except Exception as e:
				pass


class SQSWriter(Thread):
	def __init__(self, SQS):
		super(SQSWriter, self).__init__()
		self.running = True
		self.q = Queue.Queue()
		self.SQS = SQS
		self.qlen = 0

	def add(self, data):
		self.q.put(data)
		self.qlen += 1
 
	def stop(self):
		self.running = False
 
	def run(self):
		q = self.q
		SQS = self.SQS
		global idle_variable
		while self.running:
			try:
				# block for 1 second only:
				value = q.get(block=True, timeout=1)
				self.qlen -= 1
				m = Message()
				m.set_body(json.dumps(value))
				SQS.write(m)
				print("[info] Job %d written to SQS." %value[1])
				idle_variable = True

			except Queue.Empty:
				idle_variable = False
				sys.stdout.flush()
					

		if not q.empty():
			print "Elements left in the queue:"
			while not q.empty():
				print q.get()

def checkDynamoDB(clientid, jobid):
	try:
		DBtable.get_item(hash_key = clientid, range_key = str(jobid))
		return True	
	except:
		return False

def insertDynamoDB(clientid, jobid):
	try:
		item = DBtable.new_item(hash_key = clientid, range_key = str(jobid))
		item.put()
		return True
	except:
		return False

class checkIdle(Thread):
	def __init__(self):
		super(checkIdle,self).__init__()
		self.running = True
 
	def stop(self):
		self.running = False
 
	def run(self):
		global idle_variable
		while self.running:
			if (idle_variable == False):
				print ("[info] Worker idle. Waiting for %d seconds." %MAX_IDLE_TIME)
				sleep(MAX_IDLE_TIME)
				if (idle_variable == False):
					shutdown()
def shutdown():
	print("[info] Max idle time reached. Waiting for return queue to be cleared")
	while(worker_q.count() != 0):
		continue

	print("[info] Cleaning up ...")
	cleanup_all()

	print("[info] Shutting down instance ...")
	try:
		subprocess.call("sudo shutdown -h now", shell=True)
	except:
		print("[info] Failed to shutdown instance.")
		os._exit(1)
	print("[info] Instance shutted down succesfully.")
	os._exit(1)
		
#Processing incoming tasks
def process(value):
	clientid = value[0].encode()
	jobid = value[1]
	task = value[2].encode()
	status = 0  #task not done yet
	output = ""
	tasks[clientid][jobid] = [task, status, output]
	return clientid, jobid

#Function sleep
def do_sleep(clientid, jobid):
	time = float(tasks[clientid][jobid][0][6:])/1000
	try:
		sleep(time)
		tasks[clientid][jobid][1] = 1 #Success - Changing status to 1
		tasks[clientid][jobid][2] = "done"
	except Exception as e:
		print e
		tasks[clientid][jobid][1] = 2 #Error - Changing status to 2

def cleanup(t):
	t.stop()
	t.join()


def get_SQS_queue():
	i = 0
	while (i < MAX_WORKERS):
		worker_name = "worker%d" %i
		i += 1
		w = conn.get_queue(worker_name)
		if ((w != None) and (str(w) not in str(SQS_queues))):
			SQS_queues.append(w)
			print ("[info] Added new SQS queue: %s" %worker_name)
		elif (w == None):
			worker_q = conn.create_queue(worker_name)
			print ("[info] Creating new SQS queue: %s" %worker_name)
			return worker_q
	print("[info] No available queue found, retrying ...")
	return None
		
#Check for jobs finished by local workers
def retrieve_finished_jobs():
	finished_jobs = []
	for clientid in tasks.keys():
		for jobid in tasks[clientid].keys():
			if tasks[clientid][jobid][1] == 1:
				finished_jobs.append([str(clientid), jobid, str(tasks[clientid][jobid][2])])
				del tasks[clientid][jobid]
	return finished_jobs

def cleanup_all():
	print("\n")
	print("Deleting worker queue ...")
	conn.delete_queue(worker_q)			
	for i in range(num_threads):
		print("Stopping thread %d / %d ..." % (i+1, num_threads))
		workers[i].stop()
		workers[i].join()
		SQSWriters[i].stop()
		SQSWriters[i].join()
		SQSReaders[i].stop()
		SQSReaders[i].join()

#Return worker thread id with the minimum queue length
def find_min_queue(threads):
 	max_qlen = 0
	for t in threads:
		if t.qlen > max_qlen:
			max_qlen = t.qlen
	min_qlen = max_qlen
	i = 0
	j = 0
	for t in workers:
		if t.qlen < min_qlen:
			min_qlen = t.qlen
			j = i
		i += 1	
	return j

def main():
	while True:
		try:
			output = retrieve_finished_jobs()
			if output: #List is not empty
				for e in output:
					i = find_min_queue(SQSWriters)
					SQSWriters[i].add(e)

		except KeyboardInterrupt:
			print
			print "Stop."
			cleanup_all()
			break

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-i',
						'--idle',
						type = int,
						default = 0,
						metavar = '<IDLE_TIME>')
	args = parser.parse_args()


	#Connect to SQS and get main queue and create worker queue.
	try:
		conn = boto.sqs.connect_to_region("us-west-2",
										  aws_access_key_id = access_key_id,
							 	  		  aws_secret_access_key = secret_access_key)
	except:
		print ("[error] Connection to SQS failed. Check your credentials file. Exiting.")
		sys.exit(0)
	print("[info] Connection to SQS success.")

	try:
		main_q = conn.get_queue('Main')
	except:
		print("[error] Can't access SQS queue 'Main'")
	print("[info] Main SQS queue retrieved.")

	for attempt in range(10):
		try:	
			worker_q = get_SQS_queue()
			break
		except Exception as e:
			print("[info] Attempt %d/10. Worker queue not valid. Will retry in 10 seconds." % (attempt+1))
			sleep(10)
			continue

	if attempt == 9:
		print("[error] Worker queue not valid. Exiting")
		sys.exit(0)

	#Connect to DynamoDB and create table if it doesn't exist.
	try:
		conn_db = boto.dynamodb.connect_to_region('us-west-2',
                                                  aws_access_key_id = access_key_id,
                                                  aws_secret_access_key = secret_access_key)
	except:
		print ("[error] Connection to DynamoDB failed. Check your credentials file. Exiting.")
		sys.exit(0)
		
	print("[info] Creating DynamoDB table...");
	try:
		DBtable = conn_db.get_table('taskstable')
	except:
		DBfields = conn_db.create_schema(hash_key_name = 'clientid',
										 hash_key_proto_value = str,
										 range_key_name = 'jobid',
										 range_key_proto_value = str)
		DBtable  = conn_db.create_table('taskstable', DBfields, 1, 1)
		sleep(20)
	print("[info] Success accessing DynamoDB table.")
	#Starting worker threads, SQSWriters and SQSReaders
	for i in range(num_threads):
		t = LocalWorker()
		Reader = SQSReader(main_q)
		Writer = SQSWriter(worker_q) 
		workers.append(t)
		SQSReaders.append(Reader)
		SQSWriters.append(Writer)
		t.start()	
		Reader.start()
		Writer.start()
	print("[info] %d threads created and run successfully" % num_threads)

	print("[info] Waiting for tasks ...")
	if (args.idle != 0):
		MAX_IDLE_TIME = args.idle
		checkIdle_thread = checkIdle()
		checkIdle_thread.start()
	main()





