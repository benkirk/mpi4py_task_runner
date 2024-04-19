#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import os
import time



################################################################################
class Manager(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,dirs=None,options=None):
        MPIClass.__init__(self,options)
        self.iteration=0
        self.dirs = dirs
        self.num_files = 0
        self.num_dirs = 0
        self.file_size = 0
        self.niter = 10*self.comm.Get_size()
        self.any_dirs = [False for p in range(0,self.nranks)]
        self.any_dirs[0] = True
        self.progress_counts = [0 for p in range(0,self.nranks)]
        self.progress_time = self.start_time = MPI.Wtime()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def finished(self):
        self.iteration +=1

        if any(self.any_dirs): return False

        return True



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def report_progress(self, forceprint=False):
        curtime = MPI.Wtime()
        deltaprog = (curtime - self.progress_time)

        if not forceprint:
            if deltaprog < 5.: return

        self.progress_time = curtime
        elapsed = (curtime - self.start_time)
        self.progress_counts[0] = 0
        total = sum(self.progress_counts)
        self.progress_counts[0] = total
        print('{:,} items; {:,} items/sec'.format(total, int(float(total)/elapsed)))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):

        status = MPI.Status()

        # execution loop, until we determine we are finished.
        while not self.finished():

            self.any_dirs[0] = True if self.dirs else False

            #print(self.any_dirs)

            # check for incoming directories
            if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['dir_reply'], status=status):
                ready_rank = status.Get_source()
                self.any_dirs[0]          = True
                self.any_dirs[ready_rank] = False
                more_dirs  = self.comm.recv(source=ready_rank, tag=self.tags['dir_reply'])
                assert more_dirs
                self.progress_counts[ready_rank] = more_dirs.pop()
                self.dirs.extend(more_dirs)
                #print(" *** master received a dir_reply from [{:3d}] {} ***".format(ready_rank, more_dirs))
                self.report_progress()


            # check for incoming ready status
            if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status):
                ready_rank = status.Get_source()
                #print(ready_rank)
                self.any_dirs[ready_rank] = False
                self.comm.recv(source=ready_rank, tag=self.tags['ready'])
                next_dir = None
                if self.dirs:
                    next_dir  = self.dirs.pop()
                    self.any_dirs[ready_rank] = True
                    #print("Running dir {} on rank {}".format(next_dir, ready_rank))
                self.comm.send(next_dir, dest=ready_rank, tag=self.tags['execute'])



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
        self.report_progress(forceprint=True)

        return
