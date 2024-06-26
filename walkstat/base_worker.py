#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass, DirEntry, FileEntry, format_number, UIDCounts, GIDCounts
import os, sys, stat
import shutil
import queue
from typing import NamedTuple

MAXDIRS_BEFORE_SEND = 200
PROGRESS_INCREMENT = 20000

################################################################################
class BaseWorker(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)

        self.queue = None
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
            thisdir_max_mtime = -1
            thisdir_max_ctime = -1
            thisdir_max_atime = -1

            dirdepth = dirname.count(os.path.sep)

            for di in os.scandir(dirname):

                pathname = di.path

                statinfo = di.stat(follow_symlinks=False)

                thisdir_nitems += 1
                thisdir_nbytes += statinfo.st_size
                self.num_items += 1
                self.total_size += statinfo.st_size

                self.uids[statinfo.st_uid].set_id(statinfo.st_uid)
                self.uids[statinfo.st_uid].nitems += 1
                self.uids[statinfo.st_uid].nbytes += statinfo.st_size

                self.gids[statinfo.st_gid].set_id(statinfo.st_gid)
                self.gids[statinfo.st_gid].nitems += 1
                self.gids[statinfo.st_gid].nbytes += statinfo.st_size

                # send a progress update periodically
                # (in production we have many directories with 1M+ files, this ensures some progress is
                # detected and reported by Manager even for huge dirs)
                if 0 == thisdir_nitems%PROGRESS_INCREMENT:
                    self.report_progress()

                # decode file type
                fmode = statinfo.st_mode

                if stat.S_ISDIR(fmode):
                    self.dirs.append(pathname)
                    if len(self.dirs) == MAXDIRS_BEFORE_SEND: self.send_my_dirlist()
                else:
                    self.num_files += 1

                    if   stat.S_ISREG(fmode):  self.st_modes['reg']   += 1
                    elif stat.S_ISLNK(fmode):  self.st_modes['link']  += 1
                    elif stat.S_ISBLK(fmode):  self.st_modes['block'] += 1
                    elif stat.S_ISCHR(fmode):  self.st_modes['char']  += 1
                    elif stat.S_ISFIFO(fmode): self.st_modes['fifo']  += 1
                    elif stat.S_ISSOCK(fmode): self.st_modes['sock']  += 1

                    # track the *maximum mtime/ctime/atime for this directories contents (not the dir itself though)
                    thisdir_max_mtime = max(thisdir_max_mtime, statinfo.st_mtime)
                    thisdir_max_ctime = max(thisdir_max_ctime, statinfo.st_ctime)
                    thisdir_max_atime = max(thisdir_max_atime, statinfo.st_atime)

                    fe = FileEntry(pathname, statinfo.st_size,
                                   statinfo.st_mtime, statinfo.st_ctime, statinfo.st_atime)

                    # track the size & count of this file in our top heap
                    self.top_nbytes_files.add((statinfo.st_size, fe))

                    # --------------------------------------------------------------------------
                    # additional file processing - simply a placeholder stub for derived classes
                    # to do additional work on this file.
                    self.process_file(pathname, statinfo)
                    #------------------------------------


            # track the size & count of this directory in our top heaps
            de = DirEntry(dirname, thisdir_nbytes, thisdir_nitems,
                          thisdir_max_mtime, thisdir_max_ctime, thisdir_max_atime)

            self.top_nitems_dirs.add((thisdir_nitems, de))
            self.top_nbytes_dirs.add((thisdir_nbytes, de))

            if thisdir_nitems >= self.options.threshold_count or thisdir_nbytes >= self.options.threshold_size:
                if thisdir_max_mtime > 0: self.oldest_mtime_dirs.add((-thisdir_max_mtime, de)) # (-) to turn maxheap into a minheap
                if thisdir_max_atime > 0: self.oldest_atime_dirs.add((-thisdir_max_atime, de)) # (-) to turn maxheap into a minheap


        except Exception as error:
            print('[{:3d}] Cannot scan: {}'.format(self.rank, error), file=sys.stderr)
            #print('cannot scan {}'.format(dirname), file=sys.stderr)

        # if present, wait for our work queue to drain.
        # note that since each rank is wholly responsible for a given directory,
        # and our queue is populated by item count, we have not accounted yet for
        # different item sizes and their impact on queue processing time.  By
        # waiting here for our queue to drain we can more effectively load balance,
        # rather than filling up the queue with items that may take a long time to
        # process later.
        if self.queue: self.queue.join()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        print('Not implemented!')
        raise NotImplementedError
        return



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
    def report_progress(self):
        self.comm.send([self.num_items, self.total_size], dest=0, tag=self.tags['progress'])
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        self.comm.Barrier()
        status = MPI.Status()
        while True:

            # send our dir list to manager (if any)
            self.send_my_dirlist()

            # # receive instructions from Manager
            next_dir = self.comm.sendrecv([self.num_items, self.total_size],
                                          dest=0,  sendtag=self.tags['ready'],
                                          source=0, recvtag=MPI.ANY_TAG,
                                          status=status)

            if status.Get_tag() == self.tags['terminate']:
                assert (next_dir == None)
                break

            if next_dir:
                assert (status.Get_tag() == self.tags['execute'])
                self.process_directory(next_dir)

        #print('[{:3d}] *** Finished, maximum # of dirs at once: {}'.format(self.rank,
        #                                                                   format_number(self.maxnumdirs)))
        return
