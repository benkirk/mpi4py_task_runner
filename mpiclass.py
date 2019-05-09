#!/usr/bin/env python

from mpi4py import MPI
import os
import tempfile
import shutil



################################################################################
class MPIClass:

    tags ={ 'ready'     : 10,
            'execute'   : 11,
            'terminate' : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        # initialization, get 'options' data structure from rank 0
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.options = self.comm.bcast(options)
        self.init_local_dirs()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        self.cleanup()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_local_dirs(self):

        # remember the top 'rundir' where we were launched
        self.rundir = os.getcwd()
        self.local_rankdir = self.rundir
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def cleanup(self):
        return
