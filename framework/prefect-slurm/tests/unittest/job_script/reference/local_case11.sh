#!/bin/sh -l

module load module_x
module load module_y
module load module_z

cd <TMPDIR>

mpiexec.hydra /path/to/executable/a.out