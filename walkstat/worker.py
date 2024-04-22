#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass, format_number
import os, sys, stat
import shutil
#import threading
#import queue

MAXDIRS_BEFORE_SEND = 200

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

        self.dirs = []

        # last-minute check for a race condition:
        if not os.path.exists(dirname):
            print('[{:3d}] directory \'{}\' vanished'.format(self.rank, dirname), file=sys.stderr)
            return

        self.num_dirs += 1
        self.st_modes['dir'] += 1

        #print('[{:3d}](d) {}'.format(self.rank, dirname))

        try:
            thisdir_nitems = 0
            thisdir_nbytes = 0

            dirdepth = dirname.count(os.path.sep)

            for di in os.scandir(dirname):

                pathname = di.path

                statinfo = di.stat(follow_symlinks=False)

                thisdir_nitems += 1
                thisdir_nbytes += statinfo.st_size
                self.num_items += 1
                self.total_size += statinfo.st_size

                self.uid_nitems[statinfo.st_uid] += 1
                self.uid_nbytes[statinfo.st_uid] += statinfo.st_size
                self.gid_nitems[statinfo.st_gid] += 1
                self.gid_nbytes[statinfo.st_gid] += statinfo.st_size

                if di.is_dir(follow_symlinks=False):
                    self.dirs.append(pathname)
                    if len(self.dirs) == MAXDIRS_BEFORE_SEND: self.send_my_dirlist()
                else:
                    self.num_files += 1

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
            print('[{:3d}] {}'.format(self.rank, error), file=sys.stderr)
            #print('cannot scan {}'.format(dirname), file=sys.stderr)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        assert(False)
        #return
        ##print('[{:3d}](f) {}'.format(self.rank, filename))
        #
        #self.num_files += 1
        #self.total_size += statinfo.st_size
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
    def send_my_dirlist(self):
        # option 1: send dirs in batch
        if self.dirs:
            self.maxnumdirs = max(self.maxnumdirs, len(self.dirs))
            # abuse self.dirs - append our current counts, this allows manager
            # to summarize collective progress while only sending a single message
            self.dirs.append(self.num_items)
            self.dirs.append(self.total_size)
            self.comm.send(self.dirs, dest=0, tag=self.tags['dir_reply'])
            self.dirs = []
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        self.comm.Barrier()
        status = MPI.Status()
        while True:

            # send our dir list to manager (if any)
            self.send_my_dirlist()

            # signal manager we are ready for the next task.
            self.comm.ssend([self.num_items, self.total_size], dest=0, tag=self.tags['ready'])

            # receive instructions from Manager
            next_dir = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            if status.Get_tag() == self.tags['terminate']: break

            if next_dir:
                assert (status.Get_tag() == self.tags['execute'])
                self.process_directory(next_dir)

        #print('[{:3d}] *** Finished, maximum # of dirs at once: {}'.format(self.rank,
        #                                                                   format_number(self.maxnumdirs)))
        return
