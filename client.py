#!/usr/bin/env python
# client.py
 
import sys
import socket
import json
import argparse
from time import sleep
from datetime import datetime

################### GLOBAL VARIABLES ####################
default_host = socket.gethostname()
default_port = 3030
default_ip = str(default_host) + ':' + str(default_port)
tasks = []
outputs = []
TIME = 0
#########################################################

def parseip(ip):
	i = 0
	host = ''
	while (ip[i] != ':'):
		host += ip[i]
		i += 1
	port = ip[i+1:]
	return(host,int(port))

def main(ip, workload_name):
	(host, port) = parseip(ip)
	print('\nHost: ' + host + ':' + str(port))	
	print('Workload file: ' + workload_name + '\n')
	clientid = default_host
	start = datetime.now()
	client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	client.connect((host, port))
	client.settimeout(800)
	with open(workload_name, 'r') as workload:
		try:
			i = 0
			for e in workload:
				jobid = i
				task = [str(clientid), jobid, str(e.strip())]
				tasks.append(tasks)
				client.send(json.dumps(task))
				ack = client.recv(4096)
				i += 1
				print ("[info] Job %d sent" % jobid)
			print("[info] %d jobs sent to server." % i)
			size = workload.tell()
			client.send(json.dumps([str(clientid), "EOW"])) #End of workload
		except Exception as msg:
		 	 print msg

	#After sending the workload, polling outputs from server every 5s
	while True:
		sleep(3)
		client.send(json.dumps([str(clientid), "POLL"]))

		#Receiving finished outputs from server:
		#Outputs can be big, so we break them into chunks.
		#One chunk represents a list of outputs, that can go up to 100 outputs
		chunk = []
		i = 0
		while True:
			chunk = json.loads(client.recv(4096))
			if (len(chunk) == 1 and chunk[0] == "EOW"): #Go out of the loop if all available outputs are retrieved.
				break
			outputs.append(chunk)
			client.send("ack") #Acknowledgment that server can send the next chunk.
			i += 1
		print("[info] POLL: %d outputs retrieved from server" % i)
		if len(outputs) >= len(tasks):	#When all outputs have been retrieved successfuly, break the while
			TIME = (datetime.now()-start).total_seconds()
			print("[info] All outputs retrieved.")	
			print( '\033[92m' + "[info] Total time: %.2f seconds" %TIME + '\033[0m')
			client.send(json.dumps([str(clientid), "end"]))
			break

	client.close()
	'''
	print("\nOUTPUTS:")
	for i in outputs:
		print("Job ID: {:d}  Output: {:s}".format(i[0], i[1]))
	'''
#########################################################
 
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-s', 
						'--address',
						default = default_ip,
						metavar='<IP_ADRESS:PORT>')
	parser.add_argument('-w',
						'--workload',
						default = 'default.workload',
						metavar='<WORKLOAD_FILE>')
	args = parser.parse_args()
	main(args.address, args.workload)
