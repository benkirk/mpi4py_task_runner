#!/usr/bin/env python
import shutil
import numpy as np
import tarfile
import subprocess
import os, sys, copy
import threading
import queue
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

        # self.rank_up   = self.rank+1 % self.nranks
        # self.rank_down = (self.nranks-1) if self.i_am_root else (self.rank-1)
        self.last_steal = -1

        self.wt_verbose = False
        self.run_threaded = True
        self.queue = queue.Queue()
        self.do_stat = False
        self.files = []
        self.num_files = 0
        self.dirs = []
        self.num_dirs = 0
        self.file_size = 0
        self.st_modes = defaultdict(int)
        self.excess_threshold =  1
        self.starve_threshold = 10

        self.sendvals = [list() for p in range(0,self.nranks) ]
        self.assign_requests = [MPI.REQUEST_NULL for p in range(0,self.nranks) ]
        self.steal_requests  = [MPI.REQUEST_NULL for p in range(0,self.nranks) ]

        # inialize the queue from input
        self.init_queue()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        MPIClass.__del__(self)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def queue_size(self):
        # query size
        return self.queue.qsize()



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def queue_pop(self):
        # pop queue
        #val = copy.deepcopy(self.queue.get())
        val = self.queue.get()
        #self.queue.task_done()
        return val



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def queue_extend(self,listlike):
        # extend queue, under a lock
        for item in listlike:
            assert item
            self.queue.put(item)
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
        assert self.queue_size() == 0
        nfiles = max(len(self.files), self.num_files)
        ndirs  = max(len(self.dirs),  self.num_dirs)

        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank:
                if self.i_am_root: print(sep)

                #print(nrank {}, found {} files, {} dirs".format(self.rank,self.files,self.dirs))
                print("rank {}, found {} files, {} dirs".format(self.rank, nfiles, ndirs))

        nfiles_tot = self.comm.allreduce(nfiles,         MPI.SUM)
        ndirs_tot  = self.comm.allreduce(ndirs,          MPI.SUM)
        fsize_tot  = self.comm.allreduce(self.file_size, MPI.SUM)

        self.comm.Barrier()
        sys.stdout.flush()
        if self.i_am_root:
            print("{}\nTotal found {} files, {} dirs".format(sep,nfiles_tot,ndirs_tot))
            print("Total File Size = {:.5e} bytes".format(fsize_tot))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname, statinfo=None):
        self.num_dirs += 1
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo=None):
        self.num_files += 1
        if statinfo:
            self.file_size += statinfo.st_size
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def recurse(self, top, maxdepth=10**9, depth=0):
        self.scandir_recurse(top,maxdepth,depth)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def scandir_recurse(self, top, maxdepth=10**9, depth=0):

        self.process_directory(top)

        #-------------------------------------
        # python scandir implementation follows
        newdirs = []
        try:
            for di in os.scandir(top):
                f        = di.name
                pathname = di.path
                statinfo = di.stat(follow_symlinks=False) if self.do_stat else None
                if statinfo: # increment st_mode counts
                    self.st_modes[statinfo.st_mode] += 1
                if di.is_dir(follow_symlinks=False):
                    if (depth < maxdepth):
                        self.recurse(pathname, maxdepth, depth=depth+1)
                    else:
                        newdirs.append(pathname)
                else:
                    self.process_file(pathname, statinfo)

            # append to the queue, safely
            self.queue_extend(newdirs)
        except:
            print("cannot scan {}".format(top))

        return
        # end python scandir implementation
        #----------------------------------
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def listdir_recurse(self, top, maxdepth=10**9, depth=0):

        self.process_directory(top)

        #-------------------------------------
        # python listdir implementation follows
        contents = []
        try:
            contents = os.listdir(top)
        except:
            print("cannot list {}".format(top))
            return

        newdirs = []
        for f in contents:
            pathname = os.path.join(top, f)
            #print(pathname)
            try:
                statinfo = os.lstat(pathname)
                self.st_modes[statinfo.st_mode] += 1
                if S_ISDIR(statinfo.st_mode):
                    if (depth < maxdepth):
                        self.recurse(pathname, maxdepth, depth=depth+1)
                    else:
                        newdirs.append(pathname)
                else:
                    self.process_file(pathname, statinfo)
            except:
                print("cannot stat {}".format(pathname))
                continue

        # append to the queue, safely
        self.queue_extend(newdirs)
        # end python listdir implementation
        #----------------------------------
        return



    # #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # def lfs_recurse(self, top, maxdepth=10**9, depth=0):

    #     self.dirs.append(top)

    #     print(top)
    #     #--------------------------------
    #     # lfs find implementation follows
    #     find_dirs   = "cd {} && lfs find . --maxdepth 1 -type d".format(top)
    #     find_others = "cd {} && lfs find . --maxdepth 1 ! -type d".format(top)

    #     # process directories
    #     #print(find_dirs)
    #     process = subprocess.Popen(find_dirs,
    #                                shell=True,
    #                                stdout=subprocess.PIPE)
    #     while True:
    #         output = process.stdout.readline()
    #         output = output.decode('ascii')
    #         if output == '' and process.poll() is not None: break
    #         if output:
    #             output=output.rstrip('\n')
    #             if output is '.': continue
    #             output=output.lstrip('./')
    #             #print("output={}".format(output))
    #             pathname = os.path.join(top, output)
    #             #print("pathname={}".format(pathname))
    #             if (depth < maxdepth):
    #                 self.recurse(pathname, maxdepth, depth=depth+1)
    #             else:
    #                 self.queue.append(pathname)

    #     rc = process.poll()

    #     # process non-directories
    #     #print(find_others)
    #     process = subprocess.Popen(find_others,
    #                                shell=True,
    #                                stdout=subprocess.PIPE)
    #     while True:
    #         output = process.stdout.readline()
    #         output = output.decode('ascii')
    #         if output == '' and process.poll() is not None: break
    #         if output:
    #             output=output.rstrip('\n')
    #             output=output.lstrip('./')
    #             #print("output={}".format(output))
    #             pathname = os.path.join(top, output)
    #             #print("pathname={}".format(pathname))
    #             #self.files.append(pathname)
    #             self.num_files += 1

    #     rc = process.poll()

    #     #print(self.files)
    #     #print(self.queue)
    #     #----------------------------------
    #     return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_queue(self):
        if self.i_am_root:
            if len(sys.argv) == 1:
                rootdir='.'
                self.recurse(rootdir, maxdepth=0)
            else:
                for arg in sys.argv:
                    if os.path.isdir(arg):
                        self.recurse(arg, maxdepth=0)

            sep="-s"*40
            print("{}\ndir queue, {} items=\n{}".format(sep, self.queue_size(), self.queue))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def excess_work(self):
        if self.nranks == 1: return False # no excess work with no helpers!
        if self.queue_size() > self.excess_threshold:
            return (self.queue_size() - self.excess_threshold)
        return False



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def need_work(self):
        if self.queue_size() <= self.starve_threshold:
            return True
        return False



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def queue_split(self):
        part = []
        while 2*len(part) < self.queue_size():
            part.append(self.queue_pop())

        if not part:
            return None

        return part



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def progress(self,nsteps=1):
        step=0
        while self.queue_size() and step < nsteps:
            self.recurse(self.queue_pop(),
                         maxdepth=1)
            step += 1
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def progress_daemon(self):
        while True:
            item = self.queue_pop()
            if item is None: break
            self.recurse(item,
                         maxdepth=1)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def execute(self):

        # intialiaze acounting & misc vals
        my_size     = np.full(1, 1, dtype=np.int)
        global_size = np.full(1, 1, dtype=np.int)
        stole_from  = np.zeros(self.nranks, dtype=np.int)

        all_done   = False
        allreduce  = None
        recv_cnt   = 0
        inner_loop = 0
        outer_loop = 0
        total_loop = 0
        n_msg_sent = 0
        n_msg_recv = 0

        # how many outstanding work requests to allow
        max_outstanding_requests = 1
        max_requests_per_peer = 5

        # double butffering for requests
        next_assign_requests = [ MPI.REQUEST_NULL for p in range(0,self.nranks) ]
        next_steal_requests  = [ MPI.REQUEST_NULL for p in range(0,self.nranks) ]

        tstart = MPI.Wtime()
        status = MPI.Status()
        thread = None

        #-------------------------
        # single rank optimization
        if self.nranks == 1:
            while self.queue_size():
                self.progress(10**9)
            global_size[0] = self.queue_size()
            all_done = True
        # done single rank optimization
        #------------------------------



        #------------------------
        # enter nonzero size loop
        while not all_done:


            if self.run_threaded:
                thread = threading.Thread(target=self.progress_daemon)
                thread.start()


            # inner loop specific
            outer_loop += 1
            inner_loop = 0
            barrier = None
            ready_for_barrier = False
            nbc_done = all_done
            stole_from[:] = 0; stole_from[self.rank] = 1



            #-------------------
            # enter NBC work loop
            while not nbc_done:

                inner_loop += 1
                total_loop += 1



                # make progress on our own work
                if not thread:
                    self.progress(1)
                # else:
                #     thread = threading.Thread(target=self.progress)
                #     thread.start()


                # work reply?
                if self.comm.iprobe(source=MPI.ANY_SOURCE,
                                    tag=self.tags['work_reply'],
                                    status=status):
                    recv_cnt += 1
                    work = self.comm.recv(source=status.Get_source(),
                                          tag=self.tags['work_reply'])

                    # append to the queue, safely
                    if work:
                        self.queue_extend(work)



                # work request?
                if self.comm.iprobe(source=MPI.ANY_SOURCE,
                                    tag=self.tags['work_request'],
                                    status=status):
                    source = status.Get_source()
                    n_msg_recv += 1
                    ready_for_barrier = True

                    # Reply only if I have excess work.
                    share = self.queue_split()
                    if share:
                        MPI.Request.Wait(next_assign_requests[source]) # ensure we can safely access send buffer
                        self.sendvals[source] = share
                        if self.wt_verbose:
                            label = " ***" if barrier else ""
                            print("rank {:3d} satisfying {:3d}, loop (out,in,tot) = ({}, {}, {}){}".format(self.rank,
                                                                                                           source,
                                                                                                           outer_loop,
                                                                                                           inner_loop,
                                                                                                           total_loop,
                                                                                                           label))
                        next_assign_requests[source] = self.comm.issend(self.sendvals[source],
                                                                        dest=source,
                                                                        tag=self.tags['work_reply'])

                    recv_cnt += 1 # complete the receive, (empty message)
                    self.comm.recv(source=source,
                                   tag=self.tags['work_request'])



                # Do I need more work?
                if self.need_work():
                    stealrank = self.next_steal()
                    if ((stole_from[stealrank] < max_requests_per_peer) and
                        MPI.Request.Test(next_steal_requests[stealrank])):
                        stole_from[stealrank] += 1
                        n_msg_sent += 1
                        # label = " ***" if barrier else ""
                        # print("rank {:3d} requesing work from {:3d}{}".format(self.rank,
                        #                                                       stealrank,
                        #                                                       label))
                        next_steal_requests[stealrank] = self.comm.issend(None,
                                                                          dest=stealrank,
                                                                          tag=self.tags['work_request'])


                if ready_for_barrier:
                    # ibarrier bits
                    if not barrier:
                        # activate barrier when all my sends complete
                        all_sent   = MPI.Request.Testall(self.assign_requests)
                        all_stolen = MPI.Request.Testall(self.steal_requests)

                        if all_sent and all_stolen:
                            barrier = self.comm.Ibarrier()

                    # otherwise see if barrier completed
                    else:
                        nbc_done = MPI.Request.Test(barrier)

                # end NBC loop
                #-------------

            # swap double buffers
            self.steal_requests,  next_steal_requests  = next_steal_requests,  self.steal_requests
            self.assign_requests, next_assign_requests = next_assign_requests, self.assign_requests

            # signal and wait for thread
            if thread:
                self.queue.put(None);
                thread.join()
                thread = None


            #---------------------------------------------------------
            # done with NBC, we are at a consistent state across ranks.
            # wait on previous reduciton, if any
            if allreduce:
                MPI.Request.Wait(allreduce)
                allreduce = None
                all_done = False if global_size[0] else True


            # get current size for global termination criterion,
            # reduce nonblocking
            my_size[0] = self.queue_size()
            allreduce = self.comm.Iallreduce(my_size, global_size)
            # done posting temination check
            #------------------------------


        # done not all_done loop
        #-----------------------

        # signal and wait for thread
        if thread:
            self.queue.put(None);
            thread.join()


        # complete
        tstop = MPI.Wtime()

        max_steps = self.comm.allreduce(total_loop, MPI.MAX)

        self.comm.Barrier()
        sys.stdout.flush()
        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank and (recv_cnt or self.i_am_root):
                if self.i_am_root:
                    print("-"*80)
                    print("Completed in {:4f} seconds on {} ranks in {} outer loops, max {} steps".format(tstop-tstart,
                                                                                                          self.nranks,
                                                                                                          outer_loop,
                                                                                                          max_steps))
                    print("-"*80)
                print("-r-> rank {:3d} sent {:5d}, recv {:5d} messages in {:6d} total, {:3d} outer steps".format(self.rank,
                                                                                                                 n_msg_sent,
                                                                                                                 n_msg_recv,
                                                                                                                 total_loop,
                                                                                                                 outer_loop))


        # sanity checks
        assert outer_loop is self.comm.allreduce(outer_loop, MPI.MAX), "Inconsistent outer_loop count??"
        assert outer_loop is self.comm.allreduce(outer_loop, MPI.MIN), "Inconsistent outer_loop count??"
        #print(self.file_size, self.st_modes)

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
