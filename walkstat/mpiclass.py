#!/usr/bin/env python3

from mpi4py import MPI
import os
import sys
import pwd, grp
import tempfile
import shutil
import platform
from collections import defaultdict
from typing import NamedTuple
from maxheap import MaxHeap
from datetime import datetime, timezone
from parse_args import parse_options
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
class FileEntry(NamedTuple):
    path   : str
    nbytes : int
    mtime  : float
    ctime  : float
    atime  : float

class DirEntry(NamedTuple):
    path      : str
    nbytes    : int
    nitems    : int
    max_mtime : float
    max_ctime : float
    max_atime : float



################################################################################
class MPIClass:

    tags ={ 'ready'         : 10,
            'execute'       : 11,
            'work_reply'    : 20,
            'work_request'  : 21,
            'work_deny'     : 22,
            'dir_request'   : 30,
            'dir_reply'     : 31,
            'progress'      : 40,
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
        self.maxnumdirs = 0
        self.num_files = 0
        self.num_dirs = 0
        self.num_items = 0
        self.total_size = 0
        self.st_modes = defaultdict(int)
        self.uid_nitems = defaultdict(int)
        self.uid_nbytes = defaultdict(int)
        self.gid_nitems = defaultdict(int)
        self.gid_nbytes = defaultdict(int)

        self.top_nitems_dirs   = MaxHeap(self.options.heap_size)
        self.top_nbytes_dirs   = MaxHeap(self.options.heap_size)
        self.top_nbytes_files  = MaxHeap(self.options.heap_size)
        self.oldest_mtime_dirs = MaxHeap(self.options.heap_size)
        self.oldest_atime_dirs = MaxHeap(self.options.heap_size)

        return



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def __del__(self):
        self.cleanup()
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
        # self.local_rankdir = tempfile.mkdtemp(prefix='rank{}_'.format(self.rank),
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

            # sort from largest-val-to-smallest
            # https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
            result = dict(sorted(result.items(), reverse=True, key=lambda item: item[1]))
        return result



    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def summary(self, verbose=False):

        self.comm.Barrier()

        sep='-'*80

        # gather heaps
        self.top_nitems_dirs.reset   (flatten( self.comm.gather(self.top_nitems_dirs.get_list())))
        self.top_nbytes_dirs.reset   (flatten( self.comm.gather(self.top_nbytes_dirs.get_list())))
        self.top_nbytes_files.reset  (flatten( self.comm.gather(self.top_nbytes_files.get_list())))
        self.oldest_mtime_dirs.reset (flatten( self.comm.gather(self.oldest_mtime_dirs.get_list())))
        self.oldest_atime_dirs.reset (flatten( self.comm.gather(self.oldest_atime_dirs.get_list())))

        self.st_modes   = self.gather_and_sum_dict(self.st_modes)
        self.uid_nitems = self.gather_and_sum_dict(self.uid_nitems)
        self.uid_nbytes = self.gather_and_sum_dict(self.uid_nbytes)
        self.gid_nitems = self.gather_and_sum_dict(self.gid_nitems)
        self.gid_nbytes = self.gather_and_sum_dict(self.gid_nbytes)
        sys.stdout.flush()

        #--------------------------------------------------
        if verbose:
            for p in range(0,self.nranks):
                self.comm.Barrier()
                sys.stdout.flush()
                if p == self.rank:
                    if self.i_am_root:
                        print(sep)
                    else:
                        print('rank {} / {}, found {:,} files, {:,} dirs'.format(self.rank, platform.node(),
                                                                                 self.num_files, self.num_dirs))
                        for k,v in self.st_modes.items():
                            print('   {:5s} : {:,}'.format(k,v))
                        print('   {:5s} : {}'.format('size',format_size(self.total_size)))


        #------------------------------
        # done with all colletive stuff
        if not self.i_am_root: return

        #------------------------------
        # done with all colletive stuff
        # root rank summarizes results

        print(sep + '\nUser Sizes:\n' + sep)
        for k,v in self.uid_nbytes.items():
            try:
                username = pwd.getpwuid(k).pw_name
            except KeyError:
                username = '{}*'.format(k)
            print('{:>12} : {}'.format(username,format_size(v)))
        print(sep + '\nUser Counts:\n' + sep)
        for k,v in self.uid_nitems.items():
            try:
                username = pwd.getpwuid(k).pw_name
            except KeyError:
                username = '{}*'.format(k)
            print('{:>12} : {}'.format(username,format_number(v)))
        print(sep + '\nGroup Sizes:\n' + sep)
        for k,v in self.gid_nbytes.items():
            try:
                groupname = grp.getgrgid(k).gr_name
            except KeyError:
                groupname = '{}*'.format(k)
            print('{:>12} : {}'.format(groupname,format_size(v)))
        print(sep + '\nGroup Counts:\n' + sep)
        for k,v in self.gid_nitems.items():
            try:
                groupname = grp.getgrgid(k).gr_name
            except KeyError:
                groupname = '{}*'.format(k)
            print('{:>12} : {}'.format(groupname,format_number(v)))


        # summarize stat types
        print('\n'+sep)
        print('Total Count: {} items'.format(format_number(self.progress_counts[0])))
        print('Total Size:  {}'.format(format_size(self.progress_sizes[0])))
        print('Type Counts:')
        for k,v in self.st_modes.items(): print('   {:5s} : {:,}'.format(k, v))

        # summarize top files & directories
        print(sep + '\nTop Dirs (file count):\n' + sep)
        for idx,de in self.top_nitems_dirs.top(50): print('{:>10} {:>10} {}/'.format(format_number(de.nitems), format_size(de.nbytes), de.path))
        print(sep + '\nTop Dirs (size):\n' + sep)
        for idx,de in self.top_nbytes_dirs.top(50): print('{:>10} {:>10} {}/'.format(format_size(de.nbytes), format_number(de.nitems), de.path))
        print(sep + '\nTop Files (size):\n' + sep)
        for idx,fe in self.top_nbytes_files.top(50): print('{:>10} {}'.format(format_size(fe.nbytes), fe.path))
        print(sep + '\nOldest Dirs (contents mtimes):\n' + sep)
        for idx,de in self.oldest_mtime_dirs.top(50): print('{} {:>10} {:>10} {}/'.format(datetime.fromtimestamp(de.max_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                                                                                          format_size(de.nbytes), format_number(de.nitems), de.path))
        print(sep + '\nOldest Dirs (contents atimes):\n' + sep)
        for idx,de in self.oldest_atime_dirs.top(50): print('{}  {:>10} {:>10} {}/'.format(datetime.fromtimestamp(de.max_atime).strftime('%Y-%m-%d %H:%M:%S'),
                                                                                               format_size(de.nbytes), format_number(de.nitems), de.path))

        # write summary file, if requested
        if self.options.summary:
            print('\n --> Writing summary to {}'.format(self.options.summary))
            import pandas as pd

            with pd.ExcelWriter(self.options.summary) as writer:
                # directories
                l = []
                for idx,de in self.top_nitems_dirs.top(self.options.heap_size): l.append(de)
                for idx,de in self.top_nbytes_dirs.top(self.options.heap_size): l.append(de)
                #for idx,de in self.oldest_mtime_dirs.top(self.options.heap_size): l.append(de)
                #for idx,de in self.oldest_ctime_dirs.top(self.options.heap_size): l.append(de)
                #for idx,de in self.oldest_atime_dirs.top(self.options.heap_size): l.append(de)
                l = sorted(set(l), key = lambda x: x.nitems, reverse=True)
                #for it in l: print(it)
                df = pd.DataFrame(l)
                for ts in ['max_mtime', 'max_ctime', 'max_atime']: df[ts] = pd.to_datetime(df[ts], unit='s')
                print(df)
                df.to_excel(writer, sheet_name='Directories', index=False)
                del l, df

                # files
                l = []
                for idx,fe in self.top_nbytes_files.top(self.options.heap_size): l.append(fe)
                l = sorted(set(l), key = lambda x: x.nbytes, reverse=True)
                df = pd.DataFrame(l)
                for ts in ['mtime', 'ctime', 'atime']: df[ts] = pd.to_datetime(df[ts], unit='s')
                print(df)
                df.to_excel(writer, sheet_name='Files', index=False)
                del l, df

        return
