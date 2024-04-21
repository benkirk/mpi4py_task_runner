#!/usr/bin/env python3

from mpi4py import MPI
from manager import Manager
from worker import Worker
import os, sys, copy

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
assert size > 1

comm.Barrier()

# workers on ranks [1,size)
if rank:
    worker = Worker()
    worker.run()

# manager on rank 0
else:
    print('Running on {} MPI ranks'.format(size))
    dirs = []
    if len(sys.argv) == 1:
        dirs.append('.')
    else:
        newdirs = []
        for arg in sys.argv:
            if os.path.isdir(arg):
                dirs.append(arg)

    print(dirs)
    sys.stdout.flush()
    options = set() #set(['archive'])

    manager = Manager(dirs, options)
    manager.run()
