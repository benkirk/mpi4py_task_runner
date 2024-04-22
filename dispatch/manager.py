#!/usr/bin/env python3

from mpi4py import MPI
from mpiclass import MPIClass
import os
import time
import subprocess



################################################################################
class Manager(MPIClass):

    #~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,dirs=None,options=None):
        MPIClass.__init__(self,options)
        self.iteration=0
        self.dirs = dirs
        self.num_files = 0
        self.num_dirs = 0
        self.total_size = 0
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):

        status = MPI.Status()

        find_cmd = "find "
        for dir in self.dirs:
            find_cmd += dir
            find_cmd += " "
        find_cmd += "-type d"
        print(find_cmd)

        process = subprocess.Popen(find_cmd,
                                   shell=True,
                                   stdout=subprocess.PIPE)
        # execution loop
        while True:
            output = process.stdout.readline()
            output = output.decode('ascii')
            if output == '' and process.poll() is not None: break
            if output:
                output=output.rstrip('\n')
                pathname=os.path.normpath(output)

                #print(pathname)

                self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
                self.comm.send(pathname, dest=status.Get_source(), tag=self.tags['execute'])

        rc = process.poll()


        # cleanup loop, send 'terminate' tag to each worker rank in
        # whatever order they become ready.
        # Don't forget to catch their final 'result'
        print("  --> Finished dispatch, Terminating ranks")
        requests = []
        for s in range(1,self.comm.Get_size()):
            self.comm.recv(source=MPI.ANY_SOURCE, tag=self.tags['ready'], status=status)
            # send terminate tag, but no need to wait
            requests.append(
                self.comm.isend(None, dest=status.Get_source(), tag=self.tags['terminate']))

        # OK, messages sent, wait for all to complete
        MPI.Request.waitall(requests)

        return
