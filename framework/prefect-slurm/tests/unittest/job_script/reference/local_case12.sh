#!/bin/sh -l

module load module_x
module load module_y
module load module_z

export TEST1="xyz"
export TEST2="abc"

cd <TMPDIR>

mpiexec.hydra -np 300 /path/to/executable/a.out -foo 123 -bar /path/to/some/file