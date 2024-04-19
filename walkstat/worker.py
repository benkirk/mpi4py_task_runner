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

        # on first call, have master print our local config. we can do this by sending
        # a note as our first 'result'
        self.result = None

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname):

        self.num_dirs += 1
        self.st_modes['dir'] += 1

        #print("[{:3d}](d) {}".format(self.rank, dirname))

        #-------------------------------------
        # python scandir implementation follows
        self.dirs = []
        try:
            for di in os.scandir(dirname):
                f        = di.name
                pathname = di.path

                statinfo = di.stat(follow_symlinks=False)

                if di.is_dir(follow_symlinks=False):
                    self.dirs.append(pathname)
                else:
                    self.process_file(pathname, statinfo)
        except:
            print("cannot scan {}".format(dirname))

        # add the directory object itself, to get any special permissions or ACLs

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        #print("[{:3d}](f) {}".format(self.rank, filename))

        self.num_files += 1
        self.file_size += statinfo.st_size

        # decode file type
        fmode = statinfo.st_mode
        ftype = 'f'
        if   stat.S_ISREG(fmode):  ftype = 'r'; self.st_modes['reg']   += 1
        elif stat.S_ISLNK(fmode):  ftype = 'l'; self.st_modes['link']  += 1
        elif stat.S_ISBLK(fmode):  ftype = 'b'; self.st_modes['block'] += 1
        elif stat.S_ISCHR(fmode):  ftype = 'c'; self.st_modes['char']  += 1
        elif stat.S_ISFIFO(fmode): ftype = 'f'; self.st_modes['fifo']  += 1
        elif stat.S_ISSOCK(fmode): ftype = 's'; self.st_modes['sock']  += 1
        elif stat.S_ISDIR(fmode):  assert False # huh??

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        while True:

            if self.dirs:
                # abuse self.dirs - append our current counts, this allows manager
                # to summarize collective progress while only sending a single message
                self.dirs.append(sum(self.st_modes.values()))
                self.comm.ssend(self.dirs, dest=0, tag=self.tags['dir_reply'])
                self.dirs = None

            # signal Master we are ready for the next task. We can do this
            # asynchronously, without a request, because we can infer completion
            # with the subsequent recv.
            self.comm.ssend(None, dest=0, tag=self.tags['ready'])

            # receive instructions from Master
            next_dir = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            if status.Get_tag() == self.tags['terminate']: break

            if next_dir:
                assert next_dir

                tstart = MPI.Wtime()
                self.process_directory(next_dir)

        return
