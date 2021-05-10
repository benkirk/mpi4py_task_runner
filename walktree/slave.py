#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import tarfile
import os
import shutil


################################################################################
class Slave(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)

        self.instruct = None;
        self.dirs = None
        self.files = None
        self.num_files = 0
        self.num_dirs = 0
        self.file_size = 0
        self.tar = None;
        # on first call, have master print our local config. we can do this by sending
        # a note as our first 'result'
        self.result = None #" Rank {} using local directory {}".format(self.rank, self.local_rankdir)

        # # process options. open any files thay belong in shared run directory.
        # if "archive" in self.options: self.tar = tarfile.open("output-{:05d}.tar".format(self.rank), "w")

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname, statinfo=None):
        self.num_dirs += 1
        #-------------------------------------
        # python scandir implementation follows
        self.dirs = []
        self.files = []
        #print(" scanning {}".format(dirname))
        try:
            for di in os.scandir(dirname):
                f        = di.name
                pathname = di.path


                statinfo = None #di.stat(follow_symlinks=False)
                if statinfo:
                    self.st_modes[statinfo.st_mode] += 1
                if di.is_dir(follow_symlinks=False):
                    self.dirs.append(pathname)
                else:
                    self.files.append(pathname)
                    print("[{:3d}](f) {}".format(self.rank, pathname))
                    self.process_file(pathname, statinfo)
        except:
            print("cannot scan {}".format(dirname))

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo=None):
        #self.files.append(filename)
        self.num_files += 1
        if statinfo:
            self.file_size += statinfo.st_size
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

            self.comm.ssend(self.dirs, dest=0, tag=self.tags['dir_reply'])
            self.dirs = None


            # signal Master we are ready for the next task. We can do this
            # asynchronously, without a request, because we can infer completion
            # with the subsequent recv.
            self.comm.ssend(None, dest=0, tag=self.tags['ready'])

            # receive instructions from Master
            next_dir = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            if status.Get_tag() == self.tags['terminate']: break

            print("[{:3d}](d) {}".format(self.rank, next_dir))

            assert next_dir

            tstart = MPI.Wtime()
            if next_dir: self.process_directory(next_dir)

            #self.run_serial_task()
            #print("[{:3d}] {}".format(self.rank, next_dir))
            # self.result = "  rank {} completed {} in {} sec.".format(self.rank,
            #                                                          next_dir,
            #                                                          round(MPI.Wtime() - tstart,5))
        return
