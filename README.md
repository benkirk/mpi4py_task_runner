# mpi4py_task_runner
Tool to launch serial processes on a cluster and aggregate results, using mpi4py


```bash
mpiexec -n 25 ./run.py
```
## FSL setup
```bash
# Anaconda 3-4.4.0; python3
 module purge
 module load gcc slurm
 module load anaconda/3-4.4.0
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python3.6/site-packages
 which mpiexec
 which python
 
# Anaconda 4.2.0; python2
 module purge
 module load gcc slurm
 module load anaconda/4.2.0
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages
 which mpiexec
 which python
 
 # default (CentOS 7) python2
 module purge
 module load gcc slurm
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages
 which mpiexec
 which python
 
 # run locally on login node:
 mpiexec -n 20 ./run.py
 
 # run under slurm
 srun -n 150 --pty /bin/bash
```
