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
class WorkThief(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):

        MPIClass.__init__(self,initdirs=False)

        self.rank_up   = self.rank+1 % self.nranks
        self.rank_down = (self.nranks-1) if self.i_am_root else (self.rank-1)
        self.last_steal = -1 #self.rank_up

        self.instruct = None;
        self.queue = []
        self.dirs = []
        self.files = []
        self.excess_threshold =  2
        self.starve_threshold =  0
        self.sendvals = [list() for p in range(0,self.nranks) ] #defaultdict(list)
        self.assign_requests = [MPI.REQUEST_NULL for p in range(0,self.nranks) ]
        self.steal_requests  = [MPI.REQUEST_NULL for p in range(0,self.nranks) ]
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
            rootdir='.'#'/ephemeral/benkirk'
            self.recurse(rootdir, maxdepth=0)
            sep="-s"*40
            print("{}\ndir queue, {} items=\n{}".format(sep, len(self.queue), self.queue))
            print("{}\ndirs found {} items=\n{}".format(sep, len(self.dirs),  self.dirs))
            print("{}\nfiles found {} items=\n{}".format(sep,len(self.files), self.files))

            # populate initial tasks for other ranks (unnecessary complexity)?
            excess = self.excess_work()
            while excess:
                for dest in range(1,self.nranks):
                    if excess:
                        self.sendvals[dest].append(self.queue.pop())
                        excess = self.excess_work() # still?

            for dest in range(1,self.nranks):
                if self.sendvals[dest]:
                    print("sending {} entries '{}' to rank {}".format(len(self.sendvals[dest]),self.sendvals[dest],dest))
                    self.assign_requests[dest] = self.comm.issend(self.sendvals[dest], dest=dest, tag=self.tags['work_reply'])


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

        # front = self.queue[0:mid]
        # back  = self.queue[mid:]

        # if (len(front) + len(back)) != curlen:
        #     print ("q={},\nf={},\nb={}".format(self.queue, front, back))
        #     print (len(front),len(back),(len(front)+len(back)),curlen)
        #     raise Exception('error splitting queue!')

        # self.queue = back

        front = self.queue[0:mid]
        self.queue  = self.queue[mid:]

        if (len(front) + len(self.queue)) != curlen:
            raise Exception('error splitting queue!')

        return front



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def progress(self,nsteps=1):
        step=0
        while self.queue and step < nsteps:
            self.recurse(self.queue.pop(), maxdepth=1)
            step += 1
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def execute(self):

        srcs=set()

        # intialiaze acounting & misc vals
        my_size     = np.full(1, 1, dtype=np.int)
        gloabl_size = np.full(1, 1, dtype=np.int)
        stole_from  = np.zeros(self.nranks, dtype=np.int)

        recv_cnt = 0
        recv_loop = 0
        inner_loop = 0
        outer_loop = 0

        tstart = MPI.Wtime()
        status = MPI.Status()

        # enter nonzero size loop
        while gloabl_size[0]:

            # loop specific
            outer_loop += 1
            barrier = None
            nbc_done = False
            inner_loop = 0
            outstanding_work_request = False
            stole_from[:] = 0; stole_from[self.rank] = 1

            # enter work loop
            while not nbc_done:

                inner_loop += 1
                recv_loop += 1



                # single rank optimization
                if self.nranks == 1:
                    self.progress(10**6)
                    gloabl_size[0] = len(self.queue)
                    nbc_done = False if gloabl_size else True
                    continue



                # work reply?
                if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['work_reply'], status=status):
                    self.queue.extend(self.comm.recv(source=status.Get_source(),
                                                     tag=self.tags['work_reply']))
                    outstanding_work_request = False
                    recv_cnt += 1



                # make progress on our own work
                self.progress(10)



                # work request?
                if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['work_request'], status=status):
                    source = status.Get_source()
                    recv_cnt += 1

                    # complete the receive, (empty message)
                    self.comm.recv(source=source, tag=self.tags['work_request'])

                    # Reply.
                    #assert MPI.Request.Test(self.assign_requests[source]) # should be a no-op
                    MPI.Request.Wait(self.assign_requests[source]) # should be a no-op
                    # default reply, deny
                    self.sendvals[source] = None; rtag = self.tags['work_deny']
                    # unless I have excess work
                    if self.excess_work():
                        print("rank {:3d} satisfying work request from {}".format(self.rank, source))
                        self.sendvals[source] = self.split_queue(); rtag = self.tags['work_reply']

                    self.assign_requests[source] = self.comm.issend(self.sendvals[source],
                                                                    dest=source,
                                                                    tag=rtag)



                # work deny?
                if self.comm.iprobe(source=MPI.ANY_SOURCE, tag=self.tags['work_deny'], status=status):
                    recv_cnt += 1
                    outstanding_work_request = False
                    # complete the receive, (empty message)
                    self.comm.recv(source=status.Get_source(), tag=self.tags['work_deny'])



                # Do I need more work?
                if not outstanding_work_request and self.need_work():
                    stealrank = self.next_steal()
                    if stole_from[stealrank] < 1:
                        MPI.Request.Wait(self.steal_requests[stealrank]) # should be a no-op
                        stole_from[stealrank] += 1
                        outstanding_work_request = True
                        #print("rank {:3d} requesing work from {:3d}".format(self.rank, stealrank))
                        self.steal_requests[stealrank] = self.comm.issend(None, dest=stealrank, tag=self.tags['work_request'])


                # ibarrier bits
                if not barrier:

                    if not outstanding_work_request:
                        # activate barrier when all my sends complete
                        all_sent   = MPI.Request.Testall(self.assign_requests)
                        all_stolen = MPI.Request.Testall(self.steal_requests)

                        if all_sent and all_stolen:
                            barrier = self.comm.Ibarrier()

                # otherwise see if barrier completed
                else:
                    nbc_done = MPI.Request.Test(barrier)

            # done with NBC, get current size for global termination criterion
            my_size[0] = len(self.queue)
            self.comm.Allreduce(my_size, gloabl_size)

        # complete
        tstop = MPI.Wtime()

        max_steps = self.comm.allreduce(recv_loop, MPI.MAX)

        # idx, flag, msg = MPI.Request.testany(self.assign_requests)
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
                print("-r-> rank {:3d} received {:3d} messages in {:5d} steps".format(self.rank,
                                                                                      recv_cnt,
                                                                                      recv_loop))

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
