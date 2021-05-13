#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import tarfile
import os, sys, stat
import shutil

#                b       k      M      G      T
MAXSIZE_BYTES=   2  * 1024 * 1024 * 1024 * 1024;


################################################################################
class Slave(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)


        self.tar = None;
        self.tar_cnt  = 0
        self.tar_size = 0
        # on first call, have master print our local config. we can do this by sending
        # a note as our first 'result'
        self.result = None #" Rank {} using local directory {}".format(self.rank, self.local_rankdir)

        # # process options. open any files thay belong in shared run directory.
        # if "archive" in self.options:

        return


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def check_next_tarfile(self):

        if (self.tar_size > MAXSIZE_BYTES):
            self.tar.close()
            self.tar_cnt += 1
            self.tar = None

        if self.tar is None:
            self.tar = tarfile.open("output-r{:03d}-f{}.tar".format(self.rank, self.tar_cnt),
                                    "w",
                                    format=tarfile.PAX_FORMAT)
            self.tar_size = 0
        return

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname):

        self.num_dirs += 1
        self.st_modes['dir'] += 1

        print("[{:3d}](d) {}".format(self.rank, dirname))

        #-------------------------------------
        # python scandir implementation follows
        try:
            for di in os.scandir(dirname):
                f        = di.name
                pathname = di.path

                statinfo = di.stat(follow_symlinks=False)

                # cycle next tarfile if necessary
                self.check_next_tarfile()

                # skip subdirectores
                # (master will find those)
                if di.is_dir(follow_symlinks=False): continue

                # process non-directories
                self.process_file(pathname, statinfo)
        except:
            print("cannot scan {}".format(dirname))

        # add the directory object itself, to get any special permissions or ACLs
        if self.tar: self.tar.add(dirname, recursive=False)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):

        self.num_files += 1

        self.file_size += statinfo.st_size
        self.tar_size  += statinfo.st_size

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

        print("[{:3d}]({}) {}".format(self.rank, ftype, filename))

        if self.tar: self.tar.add(filename, recursive=False)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        while True:

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
