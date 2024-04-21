#!/usr/bin/env python3

from mpi4py import MPI
import os
import sys
import pwd
import tempfile
import shutil
import platform
from collections import defaultdict
from maxheap import MaxHeap
have_humanfriendly = False
try:
    import humanfriendly
    have_humanfriendly = True
except ImportError:
    pass

################################################################################
def flatten(matrix):
    if matrix: return [item for row in matrix for item in row]
    return None

def format_size(val):
    if have_humanfriendly: return humanfriendly.format_size(val)
    return '{:.5e} bytes'.format(val)

def format_number(val):
    if have_humanfriendly: return humanfriendly.format_number(val)
    return '{:,}'.format(val)

def format_timespan(val):
    if have_humanfriendly: return humanfriendly.format_timespan(val)
    return '{:.1f} seconds'.format(val)


################################################################################
class MPIClass:

    tags ={ 'ready'         : 10,
            'execute'       : 11,
            'work_reply'    : 20,
            'work_request'  : 21,
            'work_deny'     : 22,
            'dir_request'   : 30,
            'dir_reply'     : 31,
            'counts'        : 40,
            'terminate'     : 1000 }

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __init__(self,options=None):
        # initialization, get 'options' data structure from rank 0
        self.comm = MPI.COMM_WORLD
        self.rank   = self.comm.Get_rank()
        self.nranks = self.comm.Get_size()
        self.i_am_root = False if self.rank else True
        self.options = self.comm.bcast(options)

        self.dirs = None
        self.num_files = 0
        self.num_dirs = 0
        self.num_items = 0
        self.file_size = 0
        self.st_modes = defaultdict(int)
        self.uid_nitems = defaultdict(int)
        self.uid_nbytes = defaultdict(int)
        self.gid_nitems = defaultdict(int)
        self.gid_nbytes = defaultdict(int)

        self.top_nitems_dirs = MaxHeap(500)
        self.top_nbytes_dirs = MaxHeap(500)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        self.cleanup()
        self.summary()
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def init_local_dirs(self):

        # remember the top 'rundir' where we were launched
        self.rundir = os.getcwd()

        # # get specified local temporary directory, if exists.
        # # SLURM_JOB_TMPFS_TMPDIR, tmpfs ramdisk shared shared by all ranks on node
        # # SLURM_JOB_LOCAL_TMPDIR, /local/.XXXX-user shared by all ranks on node
        # local_topdir = None
        # if not local_topdir: local_topdir = os.getenv('SLURM_JOB_TMPFS_TMPDIR')
        # if not local_topdir: local_topdir = os.getenv('SLURM_JOB_LOCAL_TMPDIR')

        # # local_topdir from slurm is job specific, let's create a subdirectory
        # # for this spefific MPI rank
        # self.local_rankdir = tempfile.mkdtemp(prefix="rank{}_".format(self.rank),
        #                                       dir=local_topdir)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def cleanup(self):
        # # if we set up a local_rankdir, go back to the top workspace 'rundir'
        # # and clean up any temporary leftovers
        # if self.local_rankdir:
        #     os.chdir(self.rundir)
        #     shutil.rmtree(self.local_rankdir,ignore_errors=True)
        #     self.local_rankdir = None
        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def gather_and_sum_dict(self, my_part):
        result = defaultdict(int)
        parts = self.comm.gather(list(my_part.items()))
        if parts:
            for part in parts:
                for k,v in part:
                    result[k] += v
        return result



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def summary(self, verbose=False):

        sep="-"*80

        # gather heaps
        self.top_nitems_dirs.reset( flatten( self.comm.gather(self.top_nitems_dirs.get_list()) ) )
        self.top_nbytes_dirs.reset( flatten( self.comm.gather(self.top_nbytes_dirs.get_list()) ) )

        self.st_modes   = self.gather_and_sum_dict(self.st_modes)
        self.uid_nitems = self.gather_and_sum_dict(self.uid_nitems)
        self.uid_nbytes = self.gather_and_sum_dict(self.uid_nbytes)
        self.gid_nitems = self.gather_and_sum_dict(self.gid_nitems)
        self.gid_nbytes = self.gather_and_sum_dict(self.gid_nbytes)
        sys.stdout.flush()

        if self.i_am_root:
            print(sep)
            for k,v in self.uid_nbytes.items():
                try:
                    username = pwd.getpwuid(k)[0]
                except KeyError:
                    username = '{}*'.format(k)
                print('{:>12} : {}'.format(username,format_size(v)))

        if verbose:
            for p in range(0,self.nranks):
                self.comm.Barrier()
                sys.stdout.flush()
                if p == self.rank:
                    if self.i_am_root:
                        print(sep)
                    else:
                        print("rank {} / {}, found {:,} files, {:,} dirs".format(self.rank, platform.node(),
                                                                                 self.num_files, self.num_dirs))
                        for k,v in self.st_modes.items():
                            print("   {:5s} : {:,}".format(k,v))
                        print("   {:5s} : {}".format('size',format_size(self.file_size)))

        if self.i_am_root:
            print(sep)
            print("Totals from all ranks:")
            for k,v in self.st_modes.items():
                print("   {:5s} : {:,}".format(k, v))

        sys.stdout.flush()
        nfiles_tot = self.comm.reduce(self.num_files, MPI.SUM)
        ndirs_tot  = self.comm.reduce(self.num_dirs,  MPI.SUM)
        fsize_tot  = self.comm.reduce(self.file_size, MPI.SUM)

        if self.i_am_root:
            print("{}\nTotal found: {:,} objects = {:,} files + {:,} dirs".format(sep,
                                                                                  nfiles_tot+ndirs_tot,
                                                                                  nfiles_tot,
                                                                                  ndirs_tot))
            print("Total File Size: {}".format(format_size(fsize_tot)))

            print(sep)
            for item in self.top_nitems_dirs.top(50): print('{:>10} {}'.format(format_number(item[0]), item[1]))
            print(sep)
            for item in self.top_nbytes_dirs.top(50): print('{:>10} {}'.format(format_size(item[0]), item[1]))

        return
