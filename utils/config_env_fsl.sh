# Anaconda 3-5.0.1; python3
module purge
module load gcc slurm
module load anaconda/3-5.0.1
module load openmpi/latest
module load mpi4py/3.0.1-python${PYTHON_VERSION}



#####################
echo "PYTHON_VERSION="${PYTHON_VERSION}
echo ${PYTHONPATH}
which mpiexec
which python
