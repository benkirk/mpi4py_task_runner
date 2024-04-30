#!/usr/bin/env pthon3

from stat_worker import StatWorker
from tar_worker import TarWorker



################################################################################
def worker_factory(type='stat'):

    if   'walkstat' == type: return StatWorker()
    elif 'walktar'  == type: return TarWorker()
    else:
        print('ERROR: Unrecognized worker type: {}'.format(type))
        raise NotImplementedError

    return None
