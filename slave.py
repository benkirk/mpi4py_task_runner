#!/usr/bin/env python

from mpi4py import MPI
from mpiclass import MPIClass
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
        self.tar = None;
        # on first call, have master print our local config. we can do this by sending
        # a note as our first 'result'
        self.result = " Rank {} using local directory {}".format(self.rank, self.local_rankdir)

        # process options. open any files thay belong in shared run directory.
        if "archive" in self.options: self.tar = tarfile.open("output-{:05d}.tar".format(self.rank), "w")

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run_serial_task(self):
        self.result = None;
        # create a clean directory for the 'step' we were passed in
        # 'instruct'.  We will go into that directory to execute a task
        # we will then optionally tar the result, and clean up our mess
        if self.local_rankdir and self.instruct:
            stepdir="{}/{}".format(self.local_rankdir, self.instruct)
            os.mkdir(stepdir)
            os.chdir(stepdir)
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
            # signal Master we are ready for the next task. We can do this
            # asynchronously, without a request, because we can infer completion
            # with the subsequent recv.
            self.comm.isend(self.result, dest=0, tag=self.tags['ready'])

            # receive instructions from Master
            self.instruct = self.comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

            # choose proper action based on message tag
            if status.Get_tag() == self.tags['terminate']: return

            tstart = MPI.Wtime()
            self.run_serial_task()
            self.result = "  rank {} completed {} in {} sec.".format(self.rank,
                                                                     self.instruct,
                                                                     MPI.Wtime() - tstart)
        return
