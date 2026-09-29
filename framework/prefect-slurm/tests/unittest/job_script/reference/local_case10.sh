#!/bin/sh

export TEST1="xyz"
export TEST2="abc"

cd <TMPDIR>

mpiexec.hydra /path/to/executable/a.out