#!/usr/bin/env python
import shutil
import numpy as np
import tarfile
import os, sys
from random import randint, seed
from stat import *
from write_rand_data import *
from collections import deque
from mpi4py import MPI
from mpiclass import MPIClass




np.set_printoptions(threshold=7)

#############################################


################################################################################
class Workthief(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):

        MPIClass.__init__(self,initdirs=False)

        self.instruct = None;
        self.queue = deque()
        self.dirs = []
        self.files = []

        if self.i_am_root:
            self.recurse("testtree", maxdepth=1)
            sep="-s"*40
            print("{}\ndir queue, {} items=\n{}".format(sep, len(self.queue), self.queue))
            print("{}\ndirs found {} items=\n{}".format(sep, len(self.dirs),  self.dirs))
            print("{}\nfiles found {} items=\n{}".format(sep,len(self.files), self.files))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def recurse(self, top, maxdepth=sys.maxint, depth=0):

        for f in os.listdir(top):
            pathname = os.path.join(top, f)
            statinfo = os.stat(pathname)
            if S_ISDIR(statinfo.st_mode):
                if (depth < maxdepth):
                    self.dirs.append(pathname)
                    self.recurse(pathname, maxdepth, depth=depth+1)
                else:
                    self.queue.append(pathname)
            else:
                #print(statinfo)
                self.files.append(pathname)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process(self):

        while self.queue:
            self.recurse(self.queue.pop())

        if self.i_am_root:
            sep="-e"*40
            print("{}\ndir queue, {} items=\n{}".format(sep, len(self.queue), self.queue))
            print("{}\ndirs found {} items=\n{}".format(sep, len(self.dirs),  self.dirs))
            print("{}\nfiles found {} items=\n{}".format(sep,len(self.files), self.files))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def random_rank_in_range (self, nentries=None):

        if not nentries:
            seed(self.rank+self.nranks)
            nentries = randint(0, 2*self.nranks)
        seed(self.rank)

        vals=[]

        while len(vals) != nentries:
            vals.append(randint(0,sys.maxint) % self.nranks)

        return np.array(vals)



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def nbc(self):

        srcs=set()
        dests = self.random_rank_in_range()

        # We don't need this long term.
        send_map = np.zeros(self.nranks, dtype=np.int)
        for dest in dests: send_map[dest] += 1
        send_map = self.comm.allreduce(send_map, MPI.SUM)

        # print start message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank:
                if self.i_am_root:
                    print("-"*80)
                    print(send_map)
                    print("-"*80)
                print("-s-> rank {:3d} sending {:3d} messages to ranks {}".format(self.rank, len(dests), dests))


        sendval = np.full(10000, 12345, dtype=np.int)
        recvval = np.zeros_like(sendval)
        recv_cnt = 0
        recv_loop = 0
        requests = []
        barrier = None
        done = False

        tstart = MPI.Wtime()

        # start sends
        for dest in dests:
            r = self.comm.Issend(sendval, dest=dest, tag=100)
            requests.append(r)

        status = MPI.Status()

        # enter recv loop
        while not done:

            recv_loop += 1

            # message waiting?
            if self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=100, status=status):
                source = status.Get_source()
                srcs.add(source)
                recvval[:] = 0
                self.comm.Recv(recvval, source=source, tag=100)
                assert np.array_equal(recvval, sendval)
                assert status.Get_tag() == 100
                recv_cnt += 1

            # barrier not active
            if not barrier:
                # activate barrier when all my sends complete
                all_sent, msg = MPI.Request.testall(requests)
                if all_sent:  barrier = self.comm.Ibarrier()

            # otherwise see if barrier completed
            else:
                done, msg = MPI.Request.test(barrier)

        # complete
        tstop = MPI.Wtime()

        assert recv_cnt == send_map[self.rank]

        max_steps = self.comm.allreduce(recv_loop, MPI.MAX)

        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank:
                if self.i_am_root:
                    print("-"*80)
                    print("Completed in {:4f} seconds on {} ranks in max {} steps".format(tstop-tstart,
                                                                                          self.nranks,
                                                                                          max_steps))
                    print("-"*80)
                print("-r-> rank {:3d} received {:3d} messages from {:3d} ranks in {:5d} steps from {}".format(self.rank,
                                                                                                               recv_cnt,
                                                                                                               len(srcs),
                                                                                                               recv_loop,
                                                                                                               np.array(list(srcs))))

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run_serial_task(self):
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        self.process()
        self.comm.Barrier()
        sys.stdout.flush()
        self.nbc()
        return




################################################################################
if __name__ == "__main__":
    wt = Workthief()
    wt.run()
