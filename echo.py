#!/usr/bin/env python

from workthief2 import WorkThief as Base

################################################################################
class EchoFiles(Base):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        Base.__init__(self)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname, statinfo=None):
        Base.process_directory(self,dirname,statinfo)
        print(dirname)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo=None):
        Base.process_file(self,filename,statinfo)
        print(filename)
        return



################################################################################
if __name__ == "__main__":
    ef = EchoFiles()
    ef.run()
    ef.summary()
