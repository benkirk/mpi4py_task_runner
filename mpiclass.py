#!/usr/bin/env python

from mpi4py import MPI


################################################################################
class MPIClass:

    tags ={ 'ready'     : 1,
            'terminate' : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.options = self.comm.bcast(options)

        return
