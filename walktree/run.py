#!/usr/bin/env python3

from mpi4py import MPI
from master import Master
from slave import Slave
import os, sys, copy

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
    dirs = []
    if len(sys.argv) == 1:
        dirs.append('.')
    else:
        newdirs = []
        for arg in sys.argv:
            if os.path.isdir(arg):
                dirs.append(arg)

    print(dirs)
    options = set(['archive'])

    master = Master(dirs, options)
    master.run()
