#!/usr/bin/env python

from mpi4py import MPI
from mpiclass import MPIClass
import os



################################################################################
class Master(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        MPIClass.__init__(self,options)
        self.iteration=0
        self.niter = 10*self.comm.Get_size()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def finished(self):
        if self.iteration == self.niter:
            return True
        self.iteration += 1
        return False;



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        instruct = None
        result = None

        # execution loop, until we determine we are finished.
        while not self.finished():
            # get 'result' from any slave rank that is 'ready'.
            result = self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
            ready_rank = status.Get_source()

            # do something useful with the result.
            if result: print(result)

            # send instructions to the ready rank. For this simple example
            # this is just a string, but could be any pickleable data type
            instruct = "step_{:05d}".format(self.iteration)
            self.comm.send(instruct, dest=ready_rank, tag=self.tags['execute'])
            print("Running step {} on rank {}".format(self.iteration,ready_rank))



        # cleanup loop, send 'terminate' tag to each slave rank in
        # whatever order they become ready.
        # Don't forget to catch their final 'result'
        print("  --> Finished dispatch, Terminating ranks")
        for s in range(1,self.comm.Get_size()):
            result = self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
            if result: print(result)
            self.comm.send(None, dest=status.Get_source(), tag=self.tags['terminate'])

        return
