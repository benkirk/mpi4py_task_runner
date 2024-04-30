#!/usr/bin/env python3

from base_worker import BaseWorker



################################################################################
class StatWorker(BaseWorker):

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        BaseWorker.__init__(self)
        return

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        # No-op, the BaseWorker handles everything we need.
        # this simply here to provide a concrete implementation.
        return
