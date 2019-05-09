#!/usr/bin/env python

import socket

def write_rand_data( min_num_files=10, max_num_files=30, min_size_pow=1,
                     max_size_pow=6):
    """ Create a random number of files of random size.  The number of files
    is bracketed by [min_num_files, max_num_files] and the size of the
    files is bracketed by 10**[min_size_pow, max_size_pow] bytes.

    Default maximum size, therefore, is 10e6 bytes.
    """

    from random import randint
    from os import urandom

    n_files = randint( min_num_files, max_num_files )
    total_size = 0
    for i in range( n_files ):

        ### Generate random data
        file_size = 10**randint( min_size_pow, max_size_pow )
        total_size += file_size
        data = urandom( file_size )

        ### Name the file -- include the expected file size in the name
        if file_size >=1024000:
            # Measured in mb -- "M" in linux
            size_string = '{:d}M'.format( int(round(file_size/1024.0**2) ) )
        elif file_size >=1024:
            # Measured in kb -- "K" in linux
            size_string = '{:d}K'.format( int(round(file_size/1024.0) ) )
        else:
            # Measured in b -- "B" in linux
            size_string = '{:d}B'.format( file_size )

        file_name = 'f{:03d}.{:s}.dat'.format( i, size_string )

        ### Write the data
        with open( file_name, 'wb' ) as f:
            f.write( data )

    # write summary file
    with open ('summary.txt', 'w') as out:
        hn=socket.gethostname()
        out.write("{} wrote {} files\n".format(hn, n_files))
        out.write("{} wrote {} bytes\n".format(hn, total_size))




if __name__ == '__main__':
    write_rand_data()
