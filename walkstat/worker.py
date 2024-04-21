#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import os, sys, stat
import shutil
#import threading
#import queue



################################################################################
class Worker(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)

        #self.queue = queue.Queue(maxsize=5000)
        #self.t = threading.Thread(target=self.process_queue, daemon=True)
        #self.t.start()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname):

        self.num_dirs += 1
        self.st_modes['dir'] += 1

        #print("[{:3d}](d) {}".format(self.rank, dirname))

        requests = []
        #-------------------------------------
        # python scandir implementation follows
        self.dirs = []
        try:
            thisdir_nitems = 0
            thisdir_nbytes = 0

            dirdepth = dirname.count(os.path.sep)

            for di in os.scandir(dirname):

                self.num_items += 1

                f        = di.name
                pathname = di.path

                statinfo = di.stat(follow_symlinks=False)

                thisdir_nitems += 1
                thisdir_nbytes += statinfo.st_size

                self.uid_nitems[statinfo.st_uid] += 1
                self.uid_nbytes[statinfo.st_uid] += statinfo.st_size
                self.gid_nitems[statinfo.st_gid] += 1
                self.gid_nbytes[statinfo.st_gid] += statinfo.st_size

                if di.is_dir(follow_symlinks=False):
                    # option 1: send dirs in batch
                    #self.dirs.append(pathname)
                    # option 2: send dirs individually
                    requests.append( self.comm.issend([pathname, self.num_items, self.file_size], dest=0, tag=self.tags['dir_reply']) )
                else:
                    self.num_files += 1
                    self.file_size += statinfo.st_size

                    # decode file type
                    fmode = statinfo.st_mode
                    if   stat.S_ISREG(fmode):  self.st_modes['reg']   += 1
                    elif stat.S_ISLNK(fmode):  self.st_modes['link']  += 1
                    elif stat.S_ISBLK(fmode):  self.st_modes['block'] += 1
                    elif stat.S_ISCHR(fmode):  self.st_modes['char']  += 1
                    elif stat.S_ISFIFO(fmode): self.st_modes['fifo']  += 1
                    elif stat.S_ISSOCK(fmode): self.st_modes['sock']  += 1
                    elif stat.S_ISDIR(fmode):  assert False # huh??
                    #self.process_file(pathname, statinfo)

            # track the size & count of this directory in our top heaps
            self.top_nitems_dirs.add((thisdir_nitems, dirname))
            self.top_nbytes_dirs.add((thisdir_nbytes, dirname))

        except Exception as error:
            #print(error)
            print('cannot scan {}'.format(dirname), file=sys.stderr)

        # OK, messages sent, wait for all to complete
        MPI.Request.waitall(requests)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        assert(False)
        #return
        ##print("[{:3d}](f) {}".format(self.rank, filename))
        #
        #self.num_files += 1
        #self.file_size += statinfo.st_size
        #
        ## decode file type
        #fmode = statinfo.st_mode
        #if   stat.S_ISREG(fmode):  self.st_modes['reg']   += 1
        #elif stat.S_ISLNK(fmode):  self.st_modes['link']  += 1
        #elif stat.S_ISBLK(fmode):  self.st_modes['block'] += 1
        #elif stat.S_ISCHR(fmode):  self.st_modes['char']  += 1
        #elif stat.S_ISFIFO(fmode): self.st_modes['fifo']  += 1
        #elif stat.S_ISSOCK(fmode): self.st_modes['sock']  += 1
        #elif stat.S_ISDIR(fmode):  assert False # huh??
        #
        #return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_queue(self):

        while True:
            item = self.queue.get()
            if item:
                print("[{:3d}] {}".format(self.rank, item))
            self.queue.task_done()

            if item is None:
                print("[{:3d}] *** terminating thread ***".format(self.rank))
                assert self.queue.empty()
                break
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        while True:

            # # option 1: send dirs in batch
            # if self.dirs:
            #     # abuse self.dirs - append our current counts, this allows manager
            #     # to summarize collective progress while only sending a single message
            #     self.dirs.append(self.num_items)
            #     self.dirs.append(self.file_size)
            #     self.comm.ssend(self.dirs, dest=0, tag=self.tags['dir_reply'])
            #     self.dirs = None

            # signal Master we are ready for the next task. We can do this
            # asynchronously, without a request, because we can infer completion
            # with the subsequent recv.
            self.comm.ssend(None, dest=0, tag=self.tags['ready'])

            # receive instructions from Master
            next_dir = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            if status.Get_tag() == self.tags['terminate']: break

            if next_dir:
                assert next_dir
                self.process_directory(next_dir)

        # Done with MPI bits, tell our thread
        #self.queue.put(None)
        #if self.t.is_alive(): self.t.join()

        return
