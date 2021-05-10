#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import os
import time



################################################################################
class Master(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,dirs=None,options=None):
        MPIClass.__init__(self,options)
        self.iteration=0
        self.dirs = dirs
        self.num_files = 0
        self.num_dirs = 0
        self.file_size = 0
        self.niter = 10*self.comm.Get_size()
        self.any_dirs = [True for p in range(0,self.nranks)]
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def finished(self):
        self.iteration +=1
        if any(self.any_dirs): return False

        return True



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        instruct = None

        # execution loop, until we determine we are finished.
        while not self.finished():

            # check for incoming directories
            more_dirs = self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['dir_reply'], status=status)
            ready_rank = status.Get_source()
            if more_dirs: self.dirs.extend(more_dirs)
            self.any_dirs[ready_rank] = True if more_dirs else False
            #print(" *** master received a dir_reply from [{:3d}] ***".format(ready_rank))


            # dispatch directories to any 'ready' ranks
            if self.dirs:

                self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
                ready_rank = status.Get_source()

                # send instructions to the ready rank. For this simple example
                # this is just a string, but could be any pickleable data type
                next_dir  = self.dirs.pop()

                #print("Running dir {} on rank {}".format(next_dir, ready_rank))
                self.comm.ssend(next_dir, dest=ready_rank, tag=self.tags['execute'])
                self.any_dirs[ready_rank] = True

            self.any_dirs[0] = True if self.dirs else False

            #time.sleep(1)


        # cleanup loop, send 'terminate' tag to each slave rank in
        # whatever order they become ready.
        # Don't forget to catch their final 'result'
        print("  --> Finished dispatch, Terminating ranks")
        requests = []
        for s in range(1,self.comm.Get_size()):
            self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
            # send terminate tag, but no need to wait
            requests.append(
                self.comm.isend(None, dest=status.Get_source(), tag=self.tags['terminate']))

        # OK, messages sent, wait for all to complete
        MPI.Request.waitall(requests)

        return
