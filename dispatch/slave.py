#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import tarfile
import os
import shutil
import threading
import queue


################################################################################
class Slave(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)

        self.tar = None;
        # on first call, have master print our local config. we can do this by sending
        # a note as our first 'result'
        self.result = None #" Rank {} using local directory {}".format(self.rank, self.local_rankdir)

        # # process options. open any files thay belong in shared run directory.
        # if "archive" in self.options:
        self.tar = tarfile.open("output-{:05d}.tar".format(self.rank), "w")
        self.queue = queue.Queue(maxsize=5000)

        self.t = threading.Thread(target=self.process_queue, daemon=True)
        self.t.start()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname, statinfo=None):
        self.num_dirs += 1

        #print("[{:3d}](d) {}".format(self.rank, dirname))

        #-------------------------------------
        # python scandir implementation follows
        #print(" scanning {}".format(dirname))
        try:
            for di in os.scandir(dirname):
                f        = di.name
                pathname = di.path

                #statinfo = None
                statinfo = di.stat(follow_symlinks=False)
                if statinfo:
                    self.st_modes[statinfo.st_mode] += 1

                # skip subdirectores
                # (master will find those)
                if di.is_dir(follow_symlinks=False):
                    continue
                # process non-directories
                else:
                    self.process_file(pathname, statinfo)
        except:
            print("cannot scan {}".format(dirname))

        # add the directory object itself, to get any special permissions or ACLs
        #if self.tar: self.tar.add(dirname, recursive=False)
        self.queue.put(dirname)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo=None):
        #print("[{:3d}](f) {}".format(self.rank, filename))

        self.num_files += 1
        if statinfo: self.file_size += statinfo.st_size

        #if self.tar: self.tar.add(filename, recursive=False)
        self.queue.put(filename)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_queue(self):

        while True:
            item = self.queue.get()
            if item:
                print("[{:3d}] {}".format(self.rank, item))
                if self.tar: self.tar.add(item, recursive=False)
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

            # signal Master we are ready for the next task. We can do this
            # asynchronously, without a request, because we can infer completion
            # with the subsequent recv.
            self.comm.ssend(None, dest=0, tag=self.tags['ready'])

            #print("[{:3d}] queue size = {}".format(self.rank, self.queue.qsize()))

            # receive instructions from Master
            next_dir = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            if status.Get_tag() == self.tags['terminate']: break

            if next_dir:
                assert next_dir

                tstart = MPI.Wtime()
                self.process_directory(next_dir)

        # Done with MPI bits, tell our thread
        self.queue.put(None)
        self.t.join()
        return
