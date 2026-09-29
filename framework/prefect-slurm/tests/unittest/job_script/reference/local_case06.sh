#!/bin/sh

cd <TMPDIR>

mpiexec.hydra -np 30 --oversubscribe /path/to/executable/a.out -x 123 -y 456