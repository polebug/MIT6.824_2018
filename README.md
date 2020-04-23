# MIT6.824_2018

This repository is some code about [MIT6824 Spring 2018 Labs](http://nil.csail.mit.edu/6.824/2018/schedule.html).   
The passing of labs is as follows:  
- [x] Lab1: [MapReduce](http://nil.csail.mit.edu/6.824/2018/labs/lab-1.html)
    - [x] Part I: Map/Reduce input and output 
    - [x] Part II: single-worker word count 
    - [x] Part III: distributing MapReduce tasks
    - [x] Part IV: handling worker failures 
    - [x] Part V: inverted index generation

- [x] Lab2: [Raft](http://nil.csail.mit.edu/6.824/2018/labs/lab-raft.html)
    - [x] Part 2A: leader election and heartbeats [(note)](https://github.com/polebug/MIT6.824_2018/wiki/Lab2A-Raft:-leader-election-and-heartbeat)
    - [x] Part 2B: keep a consistent, replicated log of operations [(note)](https://github.com/polebug/MIT6.824_2018/wiki/Lab2B-Raft:-log-replication)
    - [x] Part 2C: persist

- [x] Lab3: [Fault-tolerant Key/Value Service](http://nil.csail.mit.edu/6.824/2018/labs/lab-kvraft.html)
    - [x] Part 3A: Key/value service without log compaction  
    - [x] Part 3B: Key/value service with log compaction (TODO: need to refactor code to reduce test time)  

- [x] Lab4: [Sharded Key/Value Service](http://nil.csail.mit.edu/6.824/2018/labs/lab-shard.html)
    - [x] Part 4A: The Shard Master
    - [x] Part 4B: Sharded Key/Value Server (some failed, TODO)

Reference:
1. [MapReduce paper](http://nil.csail.mit.edu/6.824/2018/papers/mapreduce.pdf)
2. [Raft paper](http://nil.csail.mit.edu/6.824/2018/papers/raft-extended.pdf)
3. [GFS paper](http://nil.csail.mit.edu/6.824/2018/papers/gfs.pdf)