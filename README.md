==============================================================================
				     Usage
==============================================================================

client.py –s <IP_ADRESS:PORT> -w <WORKLOAD>
• IP_ADRESS:PORT is the location of the front-end scheduler (should be known 
before starting the client).
• WORKLOAD is the local file (for the client) that will store the tasks that 
need to be submitted to the scheduler.

scheduler.py –s PORT –lw <NUM_WORKERS> -rw
• PORT is the server port where the clients will connect to submit tasks and 
to retrieve results.
• -lw is the switch for local worker (NUM = number of workers)
• -rw is the switch for remote worker

worker.py –i <TIME_SEC>
TIME_SEC denotes the idle time in seconds that the worker will stay active.

dp.py

sp.py –n <NB_INSTANCES> -t <INSTANCE_TYPE>

workload_gen.py <NB_TASKS> <SLEEP>
Note: <SLEEP> is in milliseconds (for 1 second task, you should specify 1000).

==============================================================================
			   Getting started tutorial
==============================================================================
=====================================
FIRST RUN
=====================================
• Create 3 VM’s on Amazon EC2 Service (1 client, 1 scheduler, 1 worker).

• Copy client.py, workload_gen.py, default.workload to the client VM.

• scp  scheduler.py, credentials.py (you should put your own AWS 
credentials in that file) to the scheduler VM.

• scp  worker.py, credentials.py (you should put your own AWS credentials in 
that file) to the workerVM.

• Install pip (sudo apt-get install pip) and boto (sudo pip-install boto) on 
the 3 machines.

• Run scheduler.py on the scheduler VM, worker.py on the worker VM, and finally
client.py on the client VM.

=====================================
STATIC RUN (multiple workers)
=====================================
• To work with multiple workers, you should create an image of the worker machine 
you created and add the AMI number in the sp.py file.

• Copy sp.py on the scheduler VM.

• Run sp.py, scheduler.py on the scheduler VM, client.py on the client VM. 
New workers will be allocated and de-allocated dynamically.

=====================================
DYNAMIC RUN (multiple workers)
=====================================
• To work with multiple workers, you should create an image of the worker machine 
you created and add the AMI number in the dp.py file.

• Copy dp.py on the scheduler VM.

• Run dp.py, scheduler.py on the scheduler VM, client.py on the client VM.

