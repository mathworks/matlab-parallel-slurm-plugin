#!/bin/sh

# Copyright 2023 The MathWorks, Inc.

usage="$(basename "$0") matlabroot [folder] -- run third-party scheduler discovery in MATLAB R2023a onwards
    matlabroot - path to the folder where MATLAB is installed
    folder     - folder to search for cluster configuration files
                 (defaults to pwd)"

# Print usage
if [ -z "$1" ] || [ "$1" = "-h" ] || [ "$1" = "--help" ] ; then
    echo "$usage"
    exit 0
fi

# MATLAB executable to launch
matlabExe="$1/bin/matlab"
if [ ! -f "${matlabExe}" ] ; then
    echo "Could not find MATLAB executable at ${matlabExe}"
    exit 1
fi

# Folder to run discovery on. If specified, wrap in single-quotes to make a MATLAB charvec.
discoveryFolder="$2"
if [ ! -z "$discoveryFolder" ] ; then
    discoveryFolder="'${discoveryFolder}'"
fi

# Command to run in MATLAB
matlabCmd="parallel.cluster.generic.discoverGenericClusters(${discoveryFolder})"

# Arguments to pass to MATLAB
matlabArgs="-nojvm -parallelserver -batch"

# Build and run system command
CMD="\"${matlabExe}\" ${matlabArgs} \"${matlabCmd}\""
eval $CMD
