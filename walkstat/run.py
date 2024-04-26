#!/usr/bin/env python3

from mpi4py import MPI
from manager import Manager
from worker import Worker
import os, sys, copy
from parse_args import parse_options

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if 0 == rank:
    args = parse_options()

assert size > 1

comm.Barrier()

# workers on ranks [1,size)
if rank:
    worker = Worker()
    worker.run()
    worker.summary()

# manager on rank 0
else:
    print('Running on {} MPI ranks'.format(size))
    sys.stdout.flush()
    manager = Manager(options=args)
    manager.run()
    manager.summary()
