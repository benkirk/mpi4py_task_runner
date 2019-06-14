#!/usr/bin/env python

from mpi4py import MPI
import os
import tempfile
import shutil



################################################################################
class MPIClass:

    tags ={ 'ready'         : 10,
            'execute'       : 11,
            'work_reply'    : 20,
            'work_request'  : 21,
            'work_deny'     : 22,
            'terminate'     : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None,initdirs=True):
        # initialization, get 'options' data structure from rank 0
        self.comm = MPI.COMM_WORLD
        self.rank   = self.comm.Get_rank()
        self.nranks = self.comm.Get_size()
        self.i_am_root = False if self.rank else True
        self.options = self.comm.bcast(options)
        if initdirs:
            self.init_local_dirs()
        else:
            self.local_rankdir = None
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        self.cleanup()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_local_dirs(self):

        # remember the top 'rundir' where we were launched
        self.rundir = os.getcwd()

        # get specified local temporary directory, if exists.
        # SLURM_JOB_TMPFS_TMPDIR, tmpfs ramdisk shared shared by all ranks on node
        # SLURM_JOB_LOCAL_TMPDIR, /local/.XXXX-user shared by all ranks on node
        local_topdir = None
        if not local_topdir: local_topdir = os.getenv('SLURM_JOB_TMPFS_TMPDIR')
        if not local_topdir: local_topdir = os.getenv('SLURM_JOB_LOCAL_TMPDIR')

        # local_topdir from slurm is job specific, let's create a subdirectory
        # for this spefific MPI rank
        self.local_rankdir = tempfile.mkdtemp(prefix="rank{}_".format(self.rank),
                                              dir=local_topdir)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def cleanup(self):
        # if we set up a local_rankdir, go back to the top workspace 'rundir'
        # and clean up any temporary leftovers
        if self.local_rankdir:
            os.chdir(self.rundir)
            shutil.rmtree(self.local_rankdir,ignore_errors=True)
            self.local_rankdir = None
        return
