#!/usr/bin/env python3

from mpi4py import MPI
import os
import sys
import tempfile
import shutil
import platform
from collections import defaultdict



################################################################################
class MPIClass:

    tags ={ 'ready'         : 10,
            'execute'       : 11,
            'work_reply'    : 20,
            'work_request'  : 21,
            'work_deny'     : 22,
            'dir_request'   : 30,
            'dir_reply'     : 31,
            'terminate'     : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        # initialization, get 'options' data structure from rank 0
        self.comm = MPI.COMM_WORLD
        self.rank   = self.comm.Get_rank()
        self.nranks = self.comm.Get_size()
        self.i_am_root = False if self.rank else True
        self.options = self.comm.bcast(options)

        self.dirs = None
        self.num_files = 0
        self.num_dirs = 0
        self.file_size = 0
        self.st_modes = defaultdict(int)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        self.cleanup()
        self.summary()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_local_dirs(self):

        # remember the top 'rundir' where we were launched
        self.rundir = os.getcwd()

        # # get specified local temporary directory, if exists.
        # # SLURM_JOB_TMPFS_TMPDIR, tmpfs ramdisk shared shared by all ranks on node
        # # SLURM_JOB_LOCAL_TMPDIR, /local/.XXXX-user shared by all ranks on node
        # local_topdir = None
        # if not local_topdir: local_topdir = os.getenv('SLURM_JOB_TMPFS_TMPDIR')
        # if not local_topdir: local_topdir = os.getenv('SLURM_JOB_LOCAL_TMPDIR')

        # # local_topdir from slurm is job specific, let's create a subdirectory
        # # for this spefific MPI rank
        # self.local_rankdir = tempfile.mkdtemp(prefix="rank{}_".format(self.rank),
        #                                       dir=local_topdir)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def cleanup(self):
        # # if we set up a local_rankdir, go back to the top workspace 'rundir'
        # # and clean up any temporary leftovers
        # if self.local_rankdir:
        #     os.chdir(self.rundir)
        #     shutil.rmtree(self.local_rankdir,ignore_errors=True)
        #     self.local_rankdir = None
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def summary(self):

        self.comm.Barrier()
        sys.stdout.flush()

        sep="-"*80

        # print end message
        for p in range(0,self.nranks):
            self.comm.Barrier()
            sys.stdout.flush()
            if p == self.rank:
                if self.i_am_root:
                    print(sep)
                else:
                    print("rank {} / {}, found {} files, {} dirs".format(self.rank, platform.node(),
                                                                         self.num_files, self.num_dirs))
                    #print(self.st_modes)

        nfiles_tot = self.comm.allreduce(self.num_files, MPI.SUM)
        ndirs_tot  = self.comm.allreduce(self.num_dirs,  MPI.SUM)
        fsize_tot  = self.comm.allreduce(self.file_size, MPI.SUM)

        self.comm.Barrier()
        sys.stdout.flush()
        if self.i_am_root:
            print("{}\nTotal found {} / {} files / {} dirs".format(sep,
                                                                   nfiles_tot+ndirs_tot,
                                                                   nfiles_tot,
                                                                   ndirs_tot))
            print("Total File Size = {:.5e} bytes".format(fsize_tot))
        return
