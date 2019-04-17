#!/usr/bin/env python

from mpi4py import MPI
from mpiclass import MPIClass
import numpy as np
import time
import tarfile
import os
import shutil
from write_rand_data import *


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

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run_serial_task(self):
        self.result = None;
        if self.local_rankdir and self.instruct:
            stepdir="{}/{}".format(self.local_rankdir, self.instruct)
            os.mkdir(stepdir)
            os.chdir(stepdir)
            time.sleep (np.random.random_sample())
            write_rand_data()
            os.chdir(self.local_rankdir)
            if self.tar:
                self.tar.add(self.instruct)
            shutil.rmtree(stepdir,ignore_errors=True)
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
            #print(self.instruct)

            if status.Get_tag() == self.tags['terminate']:
                return;

            tstart = MPI.Wtime()
            self.run_serial_task()
            self.result = "  rank {} completed {} in {} sec.".format(self.rank,
                                                                     self.instruct,
                                                                     MPI.Wtime() - tstart)
