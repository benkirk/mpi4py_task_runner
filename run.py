#!/usr/bin/env python

from mpi4py import MPI
from master import Master
from slave import Slave

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
    options = {
        '#archive'    : None, # create per-rank tarfiles
        'runlocal'  : None  # run locally when possible
    }

    master = Master(options)
    master.run()
