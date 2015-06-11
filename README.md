Job Scheduler with dynamic load-balancing (AWS)
----------------------------------------------------------------

Description
-----------
We introduce a framework based on Amazon Web Services (EC2, SQS, DynamoDB) to process tasks in a distributed way.
The purpose is to take a workload consisting in individual tasks that can be independently computed, to process these tasks on remote workers and to give the results of these computation to our client. 
This will allow a huge gain of performance for huge workloads in comparison to doing all the tasks on one single machine.

The framework consists in different entities:
![My image](http://i.imgur.com/cZriHks.jpg)
* **Client (EC2)**:  This node will  send  a  workload  file  consisting  in tasks to be  processed. These tasks are independent from each other, so that each task can be computed individually.
* **Scheduler (EC2)**: This node will take the workload file from the clients and distribute them to workers so that the computation can be done in a parallel and faster way.
* **Worker (EC2)**:  This node has  multiple  threads, each of them taking a task as input, processing it, and sending the output back to the scheduler.
* **Global  Request  Queue (SQS)**: A distributed queue running on Amazon Simple Queue Service (SQS) that  will  be  used  to communicate the tasks from the scheduler to the workers. 
* **Client Response Queue (SQS)**: Queue running on SQS that will be used to communicate the outputs of the tasks (after processing) from the worker to the scheduler.
* **Dynamic  Provisioning (Bash/Python)**:  Piece of software/script that runs separately allowing us to allocate workers dynamically by monitoring the evolution of the Global Request Queue.
* **Monitoring System (Dynamo DB)**:  This is an amazon service that will allow us to keep track of the processing of the tasks by the workers.
The  framework  will  allow  the  processing  of  huge workload  files,  allocating  workers  on-the-fly according  to  the number of tasks in the main queue.
Such a framework can be used to do image or video processing, as well as any heavy-CPU task such as scientific computing that would take much more time if processed only on one machine.
Our framework was programmed using Python.

Usage
-----

**client.py –s <IP_ADRESS:PORT> -w <WORKLOAD>**
* **IP_ADRESS:PORT** is the location of the front-end scheduler (should be known before starting the client).
* **WORKLOAD** is the local file (for the client) that will store the tasks that need to be submitted to the scheduler (for example succession of sleep tasks, see our samples.

**scheduler.py –s PORT –lw <NUM_WORKERS> -rw**
* PORT is the server port where the clients will connect to submit tasks and to retrieve results.
* -lw is the switch for local worker (NUM = number of workers)
* -rw is the switch for remote worker

**worker.py –i <TIME_SEC>**
TIME_SEC denotes the idle time in seconds that the worker will stay active.

**dp.py**

**sp.py –n <NB_INSTANCES> -t <INSTANCE_TYPE>**

**workload_gen.py <NB_TASKS> <SLEEP>**
Note: <SLEEP> is in milliseconds (for 1 second task, you should specify 1000).

Getting started tutorial
------------------------

First Run
---------
* *Create* 3 VM’s on Amazon AWS EC2 (1 client, 1 scheduler, 1 worker).
* *Copy* **client.py**, **workload_gen.py**, **default.workload** to the client VM.
* *scp*  **scheduler.py**, **credentials.py** (you should put your own AWS credentials in that file) to the **scheduler VM**.
* *scp*  **worker.py**, **credentials.py** (you should put your own AWS credentials in that file) to the **workerVM**.
* *Install* **pip** (sudo apt-get install pip) and **boto** (sudo pip-install boto) on the 3 machines.
* *Run* **scheduler.py** on the **scheduler VM**, **worker.py** on the **worker VM**, and finally **client.py** on the **client VM**.

Static Provisioning (multiple workers)
-----------------------------
* To work with multiple workers, you should *create an AMI image* of the worker machine 
you created and add the *AMI number* in the **sp.py** file.
* *Copy* **sp.py** on the **scheduler VM**.
* *Run* **sp.py**, **scheduler.py** on the **scheduler VM**, **client.py** on the **client VM**.

Dynamic Provisioning (multiple workers)
------------------------------
* To work with multiple workers, you should *create an AMI image* of the worker machine 
you created and add the *AMI number* in the **dp.py** file.
* *Copy* **dp.py** on the **scheduler VM**.
* *Run* **dp.py**, **scheduler.py** on the **scheduler VM**, **client.py** on the **client VM**. 
* New workers will be allocated and de-allocated **dynamically**: 
	* if the load is too high amongst all nodes, one node will be added.
	* if the load on some node is null, this node will be terminated.

