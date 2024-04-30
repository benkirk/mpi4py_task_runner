#!/usr/bin/env python3

import os, sys, stat
import threading
import queue
import tarfile
from base_worker import BaseWorker
from typing import NamedTuple



################################################################################
class QueueItem(NamedTuple):
    path     : str
    statinfo : stat


#                b       k      M      G      T
MAXSIZE_BYTES=   2  * 1024 * 1024 * 1024 * 1024

################################################################################
class TarWorker(BaseWorker):


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self):
        BaseWorker.__init__(self)

        self.tar = None
        self.tar_cnt  = 0
        self.tar_size = 0
        self.queue = queue.Queue(maxsize=self.options.tar_queue_depth)

        self.t = threading.Thread(target=self.process_queue, daemon=True)
        self.t.start()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def check_next_tarfile(self):

        if (self.tar_size > MAXSIZE_BYTES):
            self.tar.close()
            self.tar_cnt += 1
            self.tar = None

        if self.tar is None:
            self.tar = tarfile.open('output-r{:03d}-f{}.tar'.format(self.rank, self.tar_cnt),
                                    'w',
                                    format=tarfile.PAX_FORMAT)
            self.tar_size = 0
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_directory(self, dirname):
        #print('hello, world!! ' + dirname)

        # last-minute check for a race condition:
        if os.path.exists(dirname):
            # add the directory object itself, to get any special permissions or ACLs
            self.queue.put(QueueItem(dirname,None))

        super().process_directory(dirname)
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_file(self, filename, statinfo):
        #
        self.queue.put(QueueItem(filename,statinfo))
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def process_queue(self):

        while True:

            item = self.queue.get()

            # when we recieve a None item, that means time to go home...
            if item is None:
                self.queue.task_done()
                #print('[{:3d}] *** work queue empty, terminating thread ***'.format(self.rank))
                assert self.queue.empty()
                return

            # Roll-over our current tar file, if necessary
            self.check_next_tarfile()

            # OK, we have an item (tuple) to process.
            # item.statinfo: None for directories, a statinfo object for files.
            if item.statinfo:
                self.tar_size += item.statinfo.st_size
                #print('[{:3d}](f) {}'.format(self.rank, item.path))
            #else:
            #    print('[{:3d}](d) {}'.format(self.rank, item.path))

            assert self.tar

            try:
                self.tar.add(item.path,
                             recursive=False)

            except Exception as error:
                print('[{:3d}] Cannot add to tar file: {}'.format(self.rank, error), file=sys.stderr)
                #print('cannot scan {}'.format(dirname), file=sys.stderr)


            self.queue.task_done()

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def run(self):

        super().run()

        #print('[{:3d}] Done walking, waiting for tar queue to drain ({:,} items remaining...)'.format(self.rank,
        #                                                                                              self.queue.qsize()))

        # Done with MPI bits, tell our thread
        self.queue.put(None)
        self.t.join()
        return
