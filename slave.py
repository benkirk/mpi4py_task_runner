#!/usr/bin/env python

from mpi4py import MPI
import numpy as np
import time
import tarfile
import os
from mpiclass import MPIClass



################################################################################
class Slave(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        MPIClass.__init__(self)

        self.instruct = None;
        self.result = None;
        self.tar = None;

        # process options. open any files thay belong in shared run directory.
        if "archive" in self.options: self.tar = tarfile.open("output-{:05d}.tar".format(self.rank), "w")

        self.setup_local_rundir()
        return


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def setup_local_rundir(self):
        return;



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run_serial_task(self):
        self.result = None;
        time.sleep (np.random.random_sample())
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):
        status = MPI.Status()
        while True:
            # signal Master we are ready for the next task
            ready = self.comm.isend(self.result, dest=0, tag=self.tags['ready'])
            ready.wait()

            # receive instructions from Master
            go = self.comm.irecv(source=0, tag=MPI.ANY_TAG)
            self.instruct = go.wait(status=status)

            if status.Get_tag() == self.tags['terminate']: return;

            #print("  got {} on rank {}".format(self.instruct,self.rank))
            self.run_serial_task()
