#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import tarfile
import os, sys, stat
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
    def process_directory(self, dirname):
        self.num_dirs += 1

        #print("[{:3d}](d) {}".format(self.rank, dirname))
        self.st_modes['dir'] += 1

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
        self.queue.put(dirname)

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



    # #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # def run_serial_task(self):
    #     self.result = None;
    #     # create a clean directory for the 'step' we were passed in
    #     # 'instruct'.  We will go into that directory to execute a task
    #     # we will then optionally tar the result, and clean up our mess
    #     if self.local_rankdir and self.instruct:
    #         stepdir="{}/{}".format(self.local_rankdir, self.instruct)
    #         os.mkdir(stepdir)
    #         os.chdir(stepdir)
    #         write_rand_data()
    #         os.chdir(self.local_rankdir)
    #         if self.tar:
    #             self.tar.add(self.instruct)
    #         shutil.rmtree(stepdir,ignore_errors=True)
    #     return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        while True:


            if self.dirs:
                self.comm.ssend(self.dirs, dest=0, tag=self.tags['dir_reply'])
                self.dirs = None


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

                #self.run_serial_task()
                #print("[{:3d}] {}".format(self.rank, next_dir))
                # self.result = "  rank {} completed {} in {} sec.".format(self.rank,
                #                                                          next_dir,
                #                                                          round(MPI.Wtime() - tstart,5))

        # Done with MPI bits, tell our thread
        self.queue.put(None)
        self.t.join()
        return
