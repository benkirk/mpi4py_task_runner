#!/usr/bin/env python

import tarfile
from workthief2 import WorkThief as Base

otar = None

################################################################################
class TarFiles(Base):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        self.otar = None
        Base.__init__(self)
        self.run_threaded = True
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        if self.otar:
            self.otar.close()
        Base.__del__(self)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname, statinfo=None):
        Base.process_directory(self,dirname,statinfo)
        #print(dirname)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo=None):

        # base class processing
        Base.process_file(self,filename,statinfo)

        # open output tar file if necessary
        if not self.otar:
            self.otar = tarfile.open("/ephemeral/benkirk/test_tar_output/output-{:05d}.tar".format(self.rank), "w")

        print("[{:3d}] {}".format(self.rank,filename))
        self.otar.add(filename, recursive=False)
        return



################################################################################
if __name__ == "__main__":
    t = TarFiles()
    t.run()
    t.summary()
