#!/bin/bash

#module purge
#module load gcc cray-mpich conda
module load conda linaro-forgr
module list

export MPICC=$(which mpicc)

envname="mpi4py-${NCAR_BUILD_ENV}"
conda activate ${envname} \
    || conda env create \
             -f conda.yaml \
             -n ${envname}
