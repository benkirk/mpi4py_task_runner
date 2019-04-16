# Anaconda 3-5.0.1; python3
module purge
module load gcc slurm
module load anaconda/3-5.0.1
module load openmpi/1.10.4
export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python3.6/site-packages

# # Anaconda 4.2.0; python2
#  module purge
#  module load gcc slurm
#  module load anaconda/4.2.0
#  module load openmpi/1.10.4
#  export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages

# default (CentOS 7) python2
module purge
module load gcc slurm
module load openmpi/1.10.4
export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages




#####################
echo $PYTHONPATH
which mpiexec
which python
