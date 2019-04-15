#!/usr/bin/env python

from mpi4py import MPI
from objects import Master, Slave

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
assert size > 1

if rank == 0:
    master = Master()
    master.run()

else:
    slave = Slave()
    slave.run()
