#!/usr/bin/env python

from mpi4py import MPI
import numpy as np
import time



################
class MPIObject:

    tags ={ 'ready'     : 1,
            'terminate' : 1000 }

    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()



########################
class Master(MPIObject):

    def __init__(self):
        MPIObject.__init__(self)
        self.size = self.comm.Get_size()
        self.iteration=0
        return

    def finished(self):
        if self.iteration == 200:
            return True
        self.iteration += 1
        return False;

    def run(self):
        status = MPI.Status()
        instruct = None
        result = None

        # execution loop
        while not self.finished():
            self.comm.recv(result,   source=MPI.ANY_SOURCE,    tag=self.tags['ready'], status=status)
            self.comm.send(instruct, dest=status.Get_source(), tag=self.tags['ready'])
            print("Running step {} on rank {}".format(self.iteration,status.Get_source()))

        # cleanup loop, send 'terminate' tag to each slave rank
        for s in range(1,self.size):
            self.comm.recv(result,   source=s, tag=self.tags['ready'], status=status)
            self.comm.send(instruct, dest=s,   tag=self.tags['terminate'])
            print("  --> Terminating rank {}".format(status.Get_source()))

        return



########################
class Slave(MPIObject):

    def __init__(self):
        MPIObject.__init__(self)
        self.instruct = None;
        self.result = None;
        return

    def run_serial_task(self):
        self.result = None;
        time.sleep (np.random.random_sample())
        return

    def run(self):
        status = MPI.Status()
        while True:
            # signal Master we are ready for the next task
            ready = self.comm.isend(self.result, dest=0, tag=self.tags['ready'])
            ready.wait()

            # receive instructions from Master
            go = self.comm.irecv(self.instruct, source=0, tag=MPI.ANY_TAG)
            go.wait(status=status)

            if status.Get_tag() == self.tags['terminate']: return;

            self.run_serial_task()
