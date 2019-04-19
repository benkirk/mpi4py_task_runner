# mpi4py_task_runner
Tool to launch serial processes on a cluster and aggregate results, using mpi4py
## Table of Contents  
[FSL](#FSL)  
[europa](#europa)  
<a name="headers"/>

## FSL
### quickstart
```bash
# on a login node
mkdir /nobackup/$USER/codes
cd /nobackup/$USER/codes
git clone https://github.com/benkirk/mpi4py_task_runner.git
cd mpi4py_task_runner/
git checkout -b 0.1.1 v0.1.1
srun -n 80 --pty /bin/bash  # grab an interactive SLURM session on 80 cores

# then in the interactive shell
. utils/config_env_fsl.sh
mpiexec ./run.py
exit

# back on the login node
make list
# only if you really need to!!
make extract 

# cleanup
git clean -xdf .
```
### Configure Environment
```bash
# Anaconda 3-5.0.1; python3
 module purge
 module load gcc slurm
 module load anaconda/3-5.0.1
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python3.6/site-packages
 
# Anaconda 4.2.0; python2
 module purge
 module load gcc slurm
 module load anaconda/4.2.0
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages
 
 # default (CentOS 7) python2
 module purge
 module load gcc slurm
 module load openmpi/1.10.4
 export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python2.7/site-packages

#####################
echo $PYTHONPATH
which mpiexec
which python
 ```
 ### Execution
 ```bash
 # run locally on login node:
 . utils/config_env_fsl.sh
 mpiexec -n 20 ./run.py
 
 # run under slurm
 srun -n 56 --pty /bin/bash # get an interactive shell with 56 cores
 . utils/config_env_fsl.sh
 mpiexec ./run.py
```

## europa
### Configure Environment
```bash
### Configure Environment
# Anaconda 3-5.0.1; python3
module purge
module load gcc slurm
module load anaconda/3-5.0.1
module load mpt
export PYTHONPATH=/software/x86_64/mpi4py/3.0.1-${MPI_ID_STRING}/lib/python3.6/site-packages

#####################
echo $PYTHONPATH
which mpiexec
which python
```

### Execution
```bash
srun -n 64 --constraint=sky --pty /bin/bash -l # get an interactive shell, 2 nodes, 32 cores ea.
. utils/config_env_fsl.sh
mpiexec ./run.py
```

