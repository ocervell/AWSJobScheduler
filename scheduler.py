#!/usr/bin/env python
# scheduler.py
import sys 
import argparse
import socket
import json
import select
import Queue
import boto.sqs
from boto.sqs.message import Message
from boto.dynamodb2.table import Table
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
default_port = 3030
clients = [] 
tasks = hash() #All tasks will be stored here
option = ''
num_threads = 16
workers = []

#For remote workers only
MAX_WORKERS = 16
conn = None #Connection to SQS
main_q = None #Main queue
SQS_queues = [] #Workers queues
SQSWriters = []
checknewSQS = None
running = True
############### BACKGROUND THREADS ###############

################## Local workers #################
#The following thread(s) will be launch only if the
#argument -lw (local workers) is passed
#Retrieves tasks sent by the client and process
#them
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

#Processing incoming tasks
def process(value):
	clientid = value[0].encode()
	if new_client(clientid):
		clients.append(clientid)
		print ("[info] Added new client: %s" %clientid)
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

#Checking new client
def new_client(clientid):
	if clientid in clients:
		return False
	return True

#Clean up thread
def cleanup(t):
	t.stop()
	t.join()

def cleanup_all():
	print("\n")
	i = 0
	for t in workers:
		print("Stopping worker %d / %d ..." % (i+1, len(workers)))
		i += 1
		t.stop()
		t.join()
	if (option == 'remote'):
		i = 0
		for writers in SQSWriters:
			print("Stopping SQSWriter %d / %d ..." % (i+1, len(SQSWriters)))
			writers.stop()
			writers.join()
			i += 1
		checknewSQS.stop()
		checknewSQS.join()
		main_q.clear()
		#Delete DynamoDB table
		try:
			conn_db = boto.dynamodb.connect_to_region('us-west-2',
													  aws_access_key_id = access_key_id,
													  aws_secret_access_key = secret_access_key) 
			DBtable = conn_db.get_table('taskstable')	
			conn_db.delete_table(DBtable)
		except:
			pass
		#print("Deleting main queue ...")
		#conn.delete(main_q)		

################# Remote workers #################
#The following thread will be launch only if the
#argument -rw (remote workers) is passed
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
		while self.running:
			try:
				# block for 1 second only:
				value = q.get(block=True, timeout=1)
				process(value)
				self.qlen -= 1
				m = Message()
				m.set_body(json.dumps(value))
				SQS.write(m)
				print("[info] Job %d written to SQS." %value[1])

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
					output = json.loads(m.get_body())
					clientid = output[0].encode()
					jobid = output[1]
					output = output[2].encode()
					tasks[clientid][jobid][1] = 1 #Success - Changing status to 1
					tasks[clientid][jobid][2] = str(output)
					SQS.delete_message(m)
					print("[info] Job %d output retrieved from SQS" %jobid)
					q_length -= 1
					l +=1
			except Exception as e:
				pass
		
#Background thread that will check for new SQS_queues and add SQSReader on the fly
class checknewSQS(Thread):
	def __init__(self):
		super(checknewSQS, self).__init__()
		self.running = True

	def stop(self):
		self.running = False
			
	def run(self):
		while self.running:
			i = 0
			while (i < MAX_WORKERS):
				worker = "worker%d" %i
				i += 1
				w = conn.get_queue(worker)
				if ((w != None) and (str(w) not in str(SQS_queues))):
					SQS_queues.append(w)
					for i in range(num_threads/MAX_WORKERS + 2): #At least 2 reader threads per SQS return queue
						t = SQSReader(w)
						t.start()
						workers.append(t)
					print ("[info] Added new SQS queue: %s" %worker)

#Check for jobs finished by local workers
def retrieve_finished_jobs(clientid):
	finished_jobs = []
	for jobid in tasks[clientid].keys():
		if tasks[clientid][jobid][1] == 1:
			finished_jobs.append([jobid, tasks[clientid][jobid][2]])
			del tasks[clientid][jobid]
	return finished_jobs

#Return worker thread id with the minimum queue length
def find_min_queue(threads):
 	max_qlen = 0
	for t in threads:
		if t.qlen > max_qlen:
			max_qlen = t.qlen
	min_qlen = max_qlen
	i = 0
	j = 0
	for t in threads:
		if t.qlen < min_qlen:
			min_qlen = t.qlen
			j = i
		i += 1	
	return j	


#Main function
def main():
	#Create a connection with the client
	s = socket.socket()			# Create a socket object
	host = socket.gethostname() # Get local machine name
	s.bind((host, port))		# Bind to the port
	inputs = []
	print "Listening on port {p} ...".format(p=port)


	s.listen(5)					# Now wait for client connection.
	while True:
		try:
			client, addr = s.accept()
			ready = select.select([client,],[], [],2)
			while True:
				try:
					if ready[0]:
						try:
							data = json.loads(client.recv(4096))
						except:
							break
					
						if (len(data) == 3): #A length of 3 means that input is task
							inputs.append(data) #Retrieve tasks from client
							client.send("ack") #Send acknowledgment
							print("[info] Job %d received" % data[1])

						else:
							clientid = data[0].encode() #Retrieve client ID
							if data[1] == "POLL":
								output = []
								output = retrieve_finished_jobs(clientid)	
								#Send each output to client
								try:
									for e in output: 
										client.send(json.dumps(e))
										ack = client.recv(4096)
									print ("[info] POLL: %d outputs sent to client %s" % (len(output), clientid))
								except Exception as e:
									print e
									print ("[info] POLL: No outputs sent to client")
								client.send(json.dumps(["EOW"]))

							elif data[1] == "EOW":
								#Adding incoming tasks to be processed by workers
								for task in inputs:
									if (option == 'local'):
										i = find_min_queue(workers)
										workers[i].add(task)
									else:
										i = find_min_queue(SQSWriters)
										SQSWriters[i].add(task)

							elif data[1] == "end":
								print('\033[92m' + "[info] All outputs sent to client." + '\033[0m')
								for entry in tasks.keys():
									if entry == clientid:
										del tasks[entry]
								del inputs[:]

								#Delete DynamoDB table
								conn_db = boto.dynamodb.connect_to_region('us-west-2',
																		  aws_access_key_id = access_key_id,
																		  aws_secret_access_key = secret_access_key) 
								DBtable = conn_db.get_table('taskstable')	
								conn_db.delete_table(DBtable)
								print("\nWaiting for client connection ...")
				
				except KeyboardInterrupt:
					print
					print "Stop."
					break
		
		except KeyboardInterrupt:
			print
			print "Stop."
			cleanup_all()
			break
		
		except socket.error, msg:
			print "Socket error! %s" % msg
			break
#########################################################
 
if __name__ == "__main__":
	#Parsing arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('-s',
						'--port',
						type= int,
						default = default_port,
						metavar='<PORT>')
	parser.add_argument('-lw',
						'--local',
						type = int,
						default = 1,
						metavar='<NUM>')
	
	parser.add_argument('-rw',
						'--remote',
						action = 'store_true')

	args = parser.parse_args()

	print("\n=======================")
	port = args.port
	print("Port: %s" % args.port)
	if args.remote:
		option = 'remote'
		print("Mode: REMOTE WORKERS")
		print("=======================\n\n")
		
		#Connect to SQS and get main queue
		try:
			conn = boto.sqs.connect_to_region("us-west-2",
											  aws_access_key_id = access_key_id,
								 	  		  aws_secret_access_key = secret_access_key)
			main_q = conn.create_queue('Main')
		except:
			print("[info] Connection to SQS failed. Exiting.")
			sys.exit(0)
		print("[info] Connection to SQS success.")
	
		#Starting worker threads, SQSWriters and SQSReaders
		checknewSQS = checknewSQS()
		checknewSQS.start()

		for i in range(num_threads):
			Writer = SQSWriter(main_q) 
			SQSWriters.append(Writer)	
			Writer.start()
		print("[info] %d threads created and run successfully" % num_threads)
	
	else:
		option = 'local'
		num_threads = args.local
		print("Mode: LOCAL WORKERS")
		print("Number of threads: %s" % args.local)
		print("=======================\n\n")

		#Starting local workers
		for i in range(num_threads):
			t = LocalWorker()
			workers.append(t)
			t.start()	
		print("[info] %d threads created and run successfully" % num_threads)


	
	#Launching main function
	main()
