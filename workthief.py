#!/usr/bin/env python
import shutil
import numpy as np
import tarfile
import os, sys, copy
from random import randint, seed
from stat import *
from write_rand_data import *
from collections import defaultdict
from time import sleep
from mpi4py import MPI
from mpiclass import MPIClass




np.set_printoptions(threshold=7)


################################################################################
class WorkReq:

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,req_rank):
        self.req_rank = req_rank
        self.step=0
        return


################################################################################
class WorkThief(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):

        MPIClass.__init__(self,initdirs=False)

        self.rank_up   = self.rank+1 % self.nranks
        self.rank_down = (self.nranks-1) if self.i_am_root else (self.rank-1)
        self.last_steal = self.rank_up

        self.instruct = None;
        self.queue = []
        self.dirs = []
        self.files = []
        self.excess_threshold =  2
        self.starve_threshold =  0
        self.sendvals = defaultdict(list)
        self.requests = [MPI.REQUEST_NULL for p in range(0,self.nranks) ]
        self.init_queue()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def next_steal(self):

        self.last_steal = (self.last_steal + 1) % self.nranks
        if self.last_steal == self.rank:
            self.last_steal = (self.last_steal + 1) % self.nranks
        return self.last_steal



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def summary(self):

        self.comm.Barrier()
        sys.stdout.flush()

        sep="-"*80
        assert len(self.queue) == 0
        nfiles = len(self.files)
        ndirs  = len(self.dirs)

        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank:
                if self.i_am_root: print(sep)

                #print(nrank {}, found {} files, {} dirs".format(self.rank,self.files,self.dirs))
                print("rank {}, found {} files, {} dirs".format(self.rank, nfiles, ndirs))

        nfiles_tot = self.comm.allreduce(nfiles, MPI.SUM)
        ndirs_tot  = self.comm.allreduce(ndirs, MPI.SUM)
        self.comm.Barrier()
        sys.stdout.flush()
        if self.i_am_root:
            print("{}\nTotal found {} files, {} dirs".format(sep,nfiles_tot,ndirs_tot))

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def recurse(self, top, maxdepth=10**9, depth=0):

        self.dirs.append(top)

        for f in os.listdir(top):
            pathname = os.path.join(top, f)
            statinfo = os.stat(pathname)
            if S_ISDIR(statinfo.st_mode):
                if (depth < maxdepth):
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
            rootdir='.'
            self.recurse(rootdir, maxdepth=0)
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
                    self.requests[dest] = self.comm.issend(self.sendvals[dest], dest=dest, tag=self.tags['work_reply'])


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
        if len(self.queue) <= self.starve_threshold:
            return True
        return False



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def split_queue(self):

        curlen = len(self.queue)
        if curlen < 2: return None

        mid = int(curlen/2)

        if mid == 0: return None

        front = self.queue[0:mid]
        back  = self.queue[mid:]

        if (len(front) + len(back)) != curlen:
            print ("q={},\nf={},\nb={}".format(self.queue, front, back))
            print (len(front),len(back),(len(front)+len(back)),curlen)
            raise Exception('error splitting queue!')

        self.queue = back
        return front



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def progress(self,nsteps=1):

        for step in range(0,nsteps):
            if self.queue:
                self.recurse(self.queue.pop(), maxdepth=1)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def random_rank (self):
        if self.nranks == 1: return 0
        rrank = self.rank
        while rrank != self.rank:
            rrank = randint(0,(self.nranks-1))
        return rrank



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def random_rank_in_range (self, nentries=None):

        if not nentries:
            seed(self.rank+self.nranks)
            nentries = randint(0, 10*self.nranks)

        if 'rand_initialized' not in WorkThief.__dict__:
            seed(self.rank)
            WorkThief.rand_initialized = True

        vals=np.empty(nentries, dtype=np.int)

        for idx in range(0,nentries):
            vals[idx] = randint(0,10**9) % self.nranks

        return vals



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def execute(self):

        srcs=set()

        # intialiaze acounting & misc vals
        my_size     = np.full(1, 1, dtype=np.int)
        gloabl_size = np.full(1, 1, dtype=np.int)

        recv_cnt = 0
        recv_loop = 0
        inner_loop = 0
        outer_loop = 0

        wr_out = WorkReq(self.rank)
        my_work_request = MPI.REQUEST_NULL

        tstart = MPI.Wtime()
        status = MPI.Status()

        # idx, flag, msg = MPI.Request.testany(self.requests)
        # print(idx, flag, msg)
        # assert not flag
        # assert idx == MPI.UNDEFINED
        # self.comm.Barrier()

        # enter nonzero size loop
        while gloabl_size[0]:

            # loop specific
            outer_loop += 1
            allreduce = None
            done = False
            looped = False
            inner_loop = 0
            i_requested_work = False

            # enter work loop
            while not done:

                inner_loop += 1
                recv_loop += 1

                # work reply?
                if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['work_reply'], status=status):
                    # probe the source and tag before receiving
                    source = status.Get_source()
                    srcs.add(source)

                    # complete the receive
                    recvval = self.comm.recv(source=source, tag=self.tags['work_reply'])
                    recv_cnt += 1
                    if recvval:
                        #print("{:3d} rank got '{}' from rank {:3d}".format(self.rank,recvval,source))
                        self.queue.extend(recvval)
                        i_requested_work = False
                        looped = False

                # make progress on our own work
                self.progress()

                # Do I need more work?
                if self.nranks > 1:
                    if self.need_work() and not i_requested_work:
                        stealfrom = self.next_steal()
                        print("rank {:3d} requesing work for {:3d} from {:3d}".format(self.rank, self.rank, stealfrom))
                        # abuse requests[self.rank]
                        MPI.Request.Wait(self.requests[self.rank])
                        self.requests[self.rank] = self.comm.issend(None, dest=stealfrom, tag=self.tags['work_request'])
                        i_requested_work = True

                # work request?
                if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['work_request'], status=status):
                    # probe the source and tag before receiving
                    source = status.Get_source()
                    srcs.add(source)

                    recv_cnt += 1

                    # if we can satisfy the request, lets send the reply first. This makes sure the
                    # new message is in flight before the sending request completes
                    if self.excess_work():
                        MPI.Request.Wait(self.requests[source])
                        self.sendvals[source] = self.split_queue()
                        print("rank {:3d} satisfying work request from {}".format(self.rank, source))
                        self.requests[source] = self.comm.issend(self.sendvals[source],
                                                                 dest=source,
                                                                 tag=self.tags['work_reply'])

                    # complete the request, if we had work we sent it, otherwise ignore
                    reqval = self.comm.recv(source=source, tag=self.tags['work_request'])



                # iallreduce bits
                if not allreduce:
                    # activate barrier when all my sends complete
                    all_sent, msg = MPI.Request.testall(self.requests)
                    if all_sent:
                        my_size[0] = len(self.queue)
                        allreduce = self.comm.Iallreduce(my_size, gloabl_size)

                # otherwise see if allreduce completed
                else:
                    done, msg = MPI.Request.test(allreduce)
                    #if done and self.i_am_root: print("step {} completed with {} remaining".format(outer_loop, gloabl_size[0]))

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
                    print("{} outer loops completed".format(outer_loop))
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
        self.execute()
        return




################################################################################
if __name__ == "__main__":
    wt = WorkThief()
    wt.run()
    wt.summary()
