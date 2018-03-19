==============
 slurm_hog.py
==============

slurm_hog.py is a simple python utility for queuing serial jobs to a HPC 
cluster that only supports allocating entire nodes. This is done by launching
jobs to a backend queue that then run batches of jobs queued with this tool. 
As a bonus the backend jobs will continuiously launch jobs on its node until a
configurable amount of walltime remains, and then exit. This keep underutilized
cores to a minimum. 

A sqlite3 database is used to store job information and communicate between 
nodes. This requires cluster-wide flock to be enabled. Without this, sqlite 
will not be able to synchronize writes and bad things will happen.

Dependencies
============

* Python 3
* sqlite3
* apsw

Usage
=====

slurm_hog.py should be on your $PATH for ease of use. Functionality is provided
in subcommands. Before doing anything one should create a databse with `init`.
Basic batch queueing functionality is provided by the subcommands: `submit`, 
`check`, `cancel`, `show`. Once jobs are queued, the `monitor` subcommand is
is used to to maintain a fixed number of `hog` jobs on the queue. The submitted
`hog` jobs are tracked in the database, and if the `monitor` is terminated they
will both contiue to run and be sensed by a future `monitor` command. 

When launching `monitor` one must specify the `--command-prefix` used to launch
jobs to the backend queue. At a minimum this will be the command `srun` but if
additional options to `srun` are required (or you use a different queueing 
system) pass the full command and argument as a quoted string. By default the
prefix is `nohup` which will launch the `hog` jobs locally for testing.

See the --help for each subcommand more information and configuration options.

Scalability
===========

If the jobs finish very fast, there may be load issues with sqlite as the
database file must be locked for every write. Similarly if the filesystem is 
very slow one may encounter issues. The default database timeout is 300s and
increasing this may help avoid issues.
