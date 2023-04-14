## Drove - Distributed Job Scheduler

The Drove, manages go routines across many worker nodes. It depends on zookeeper for knowing the 
state of the cluster. 

Zookeeper is an in-memory with persistent back up distributed database, which stores info in directory style.
For example one can create a znode (a directory) /abc with value "test". When we query /abc it retrieves the value "test".
One can nest znodes like directory /abc/ghj.

There are two types of znodes: permanent and ephemeral. Permanent znodes as the name suggest stays forever in Zookeeper, 
while the emphemeral znode lasts till the end of the session of the client which created it.

Additionally Zookeeper guarantees that one cannot create duplicate directories.

The Drove leverages these properties for things like leader election and ensuring jobs are only picked once.

The scheduler has a leader chosen among the pool of workers. And each worker has a limited capacity of jobs it can handle.

To find the leader of the group workers, on startup each worker contends to create /leader ephemeral znode under /election znode.
Since Zookeeper ensures that no duplicate entries are created, only one of the worker becomes a leader. And all the workers register 
a 'watch' on /election node so that if the leader fails rest of the workers can participate in re-election.

Additionally each worker registers an ephemeral znode themselves under /live_nodes directory, with value being the host and port to reach them.
Each worker exposes two apis, one to assign a job and the other to stop a job from executing.

For a job to be worked on can use the cluster client to add a job; which in turn gets stored under /jobs directory (Example /jobs/testjob).
The leader periodically polls the job directory and assigns to the workers which have sufficient capacity using the worker's assign api.
The assigned worker marks the job as assigned by creating an ephemeral node under the job (ex /jobs/testjob/assigned).

During the course of execution of the job the worker node might die, then jobs are unassigned, since the ephemeral subdirectory /assigned
is removed by Zookeeper when the session ends. The leader assigns theses jobs to some other nodes.

One can expect network failures where the worker/leader node loses connectivity with the rest of the cluster. Zookeeper upon not receiving 
heart beat from these nodes, removes all ephemeral nodes registered by them, there by returning leadership and jobs back. Workers can detect network
failures via callbacks from the zookeeper client and shuts down leadership activities and zaps all worker goroutines. Once the 
connectivity is back up the worker registers itself to the cluster and is ready to be assigned work.
