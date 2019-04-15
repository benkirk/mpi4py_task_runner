#!/usr/bin/env python

from mpi4py import MPI
from objects import Master, Slave

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
assert size > 1

# slaves on ranks [1,size)
if rank:
    slave = Slave()
    slave.run()

# master on rank 0
else:
    options = set(['archive'])
    master = Master(options)
    master.run()
