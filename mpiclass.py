#!/usr/bin/env python

from mpi4py import MPI
import os
import tempfile
import shutil



################################################################################
class MPIClass:

    tags ={ 'ready'     : 1,
            'terminate' : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.options = self.comm.bcast(options)
        self.init_environment()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        os.chdir(self.rundir)
        if self.local_rankdir: shutil.rmtree(self.local_rankdir)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_environment(self):

        self.rundir = os.getcwd()

        # get specified local temporary directory, if exists
        local_topdir = os.getenv('SLURM_JOB_TMPDIR')

        self.local_rankdir = tempfile.mkdtemp(prefix="rank{}_".format(self.rank),
                                              dir=local_topdir)

        print(" Rank {} using local directory {}".format(self.rank, self.local_rankdir))
        return
