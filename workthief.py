#!/usr/bin/env python
import shutil
import numpy as np
import tarfile
import os, sys
from random import randint, seed
from stat import *
from write_rand_data import *
from collections import defaultdict
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
        self.queue = []
        self.dirs = []
        self.files = []
        self.excess_threshold = 10
        self.starve_threshold =  0
        self.sendvals = defaultdict(list)
        self.requests = []
        self.init_queue()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def recurse(self, top, maxdepth=10**9, depth=0):

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
    def init_queue(self):
        if self.i_am_root:
            self.recurse("testtree", maxdepth=1)
            sep="-s"*40
            print("{}\ndir queue, {} items=\n{}".format(sep, len(self.queue), self.queue))
            print("{}\ndirs found {} items=\n{}".format(sep, len(self.dirs),  self.dirs))
            print("{}\nfiles found {} items=\n{}".format(sep,len(self.files), self.files))

            # populate initial tasks for other ranks
            excess = self.excess_work()
            while excess:
                for dest in range(1,self.nranks):
                    if excess:
                        self.sendvals[dest].append(self.queue.pop())
                        excess = self.excess_work() # still?

            for dest in range(1,self.nranks):
                if self.sendvals[dest]:
                    print("sending {} entries '{}' to rank {}".format(len(self.sendvals[dest]),self.sendvals[dest],dest))
                    r = self.comm.issend(self.sendvals[dest], dest=dest, tag=self.tags['work_assign'])
                    self.requests.append(r)


            print("{}\ndir queue, {} items=\n{}".format(sep, len(self.queue), self.queue))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def excess_work(self):
        if self.nranks == 1: return False # no excess work with no helpers!
        if len(self.queue) > self.excess_threshold:
            return (len(self.queue) - self.excess_threshold)
        return False



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def need_work(self):
        if self.nranks == 1: return False # don't gather work without sources!
        if len(self.queue) <= self.starve_threshold:
            return True
        return False



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
            nentries = randint(0, 10*self.nranks)
        seed(self.rank)

        vals=np.empty(nentries, dtype=np.int)

        for idx in range(0,nentries):
            vals[idx] = randint(0,10**9) % self.nranks

        return vals



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def nbc(self):

        srcs=set()
        dests= [] #self.random_rank_in_range()

        # print start message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank and len(dests):
                if self.i_am_root:
                    print("-"*80)
                print("-s-> rank {:3d} sending {:3d} messages to ranks {}".format(self.rank, len(dests), dests))


        sendval = "foobar"
        recv_cnt = 0
        recv_loop = 0
        barrier = None
        done = False

        tstart = MPI.Wtime()

        # start sends
        for dest in dests:
            r = self.comm.issend(sendval, dest=dest, tag=self.tags['work_assign'])
            self.requests.append(r)

        status = MPI.Status()

        # idx, flag, msg = MPI.Request.testany(self.requests)
        # print(idx, flag, msg)
        # assert not flag
        # assert idx == MPI.UNDEFINED
        # self.comm.Barrier()

        # enter recv loop
        while not done:

            recv_loop += 1

            # message waiting?
            if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                source = status.Get_source()
                tag = status.Get_tag()
                srcs.add(source)
                recvval = None
                recvval = self.comm.recv(source=source, tag=tag)
                if sendval != recvval: print("{:3d} rank got '{}'".format(self.rank,recvval))
                recv_cnt += 1

            # barrier not active
            if not barrier:
                # activate barrier when all my sends complete
                # if self.i_am_root: print(MPI.Request.testany(self.requests))
                all_sent = MPI.Request.Testall(self.requests)
                if all_sent:  barrier = self.comm.Ibarrier()

            # otherwise see if barrier completed
            else:
                done = MPI.Request.Test(barrier)

        # complete
        tstop = MPI.Wtime()

        max_steps = self.comm.allreduce(recv_loop, MPI.MAX)

        # idx, flag, msg = MPI.Request.testany(self.requests)
        # print(idx, flag)
        # assert idx == MPI.UNDEFINED
        # assert flag

        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank and (recv_cnt or self.i_am_root):
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
        #self.process()
        self.comm.Barrier()
        sys.stdout.flush()
        self.nbc()
        return




################################################################################
if __name__ == "__main__":
    wt = Workthief()
    wt.run()
