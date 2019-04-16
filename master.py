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

        # process options
        # (none)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def finished(self):
        if self.iteration == 200:
            return True
        self.iteration += 1
        return False;



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        instruct = None
        result = None

        # execution loop
        while not self.finished():
            self.comm.recv(result,   source=MPI.ANY_SOURCE,    tag=self.tags['ready'], status=status)
            instruct = self.iteration
            self.comm.send(instruct, dest=status.Get_source(), tag=self.tags['ready'])
            print("Running step {} on rank {}".format(self.iteration,status.Get_source()))

        # cleanup loop, send 'terminate' tag to each slave rank
        print("  --> Terminating ranks")
        for s in range(1,self.comm.Get_size()):
            self.comm.recv(result,   source=s, tag=self.tags['ready'], status=status)
            self.comm.send(instruct, dest=s,   tag=self.tags['terminate'])

        return
