#!/bin/bash -l

# submit like:
# sbatch qscript.slurm

#SBATCH --job-name=task_runner      # Job name
#SBATCH --nodes=4                   # Use one node
#SBATCH --ntasks-per-node=28        # requesting 28 tasks/"cores" per node
#SBATCH --mem-per-cpu=1gb           # Memory per processor
#SBATCH --time=00:10:00             # Time limit hrs:min:sec
#SBATCH --output=task_runner.out    # Standard output and error log


[ -f . utils/config_env_fsl.sh ] && . utils/config_env_fsl.sh

echo " -------------------------------------------------------------------------------"
echo "| Started:       `date`"
echo "| Case name:     $SLURM_JOB_NAME"
echo "| Job ID:        $SLURM_JOBID"
echo "| Nodes:         $SLURM_JOB_NODELIST"
echo "| Working Dir:   $SLURM_SUBMIT_DIR"
echo "| # of Procs:    $SLURM_NPROCS"
echo "| # of Nodes:    $SLURM_NNODES"
echo "| # Tasks/Node:  $SLURM_TASKS_PER_NODE"
echo "|"
echo "| SLURM_ARRAY_JOB_ID=$SLURM_ARRAY_JOB_ID"
echo "| SLURM_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID"
echo "| SLURM_ARRAY_TASK_COUNT=$SLURM_ARRAY_TASK_COUNT"
echo "| SLURM_ARRAY_TASK_MIN=$SLURM_ARRAY_TASK_MIN"
echo "| SLURM_ARRAY_TASK_MAX=$SLURM_ARRAY_TASK_MAX"
echo " -------------------------------------------------------------------------------"

echo "Executing on hosts:"
srun hostname | sort | uniq

make clean
mpiexec ./run.py


echo " -------------------------------------------------------------------------------"
echo "finished on $(date)"
