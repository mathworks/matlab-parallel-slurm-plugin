function createSubmitScript(outputFilename, jobName, quotedLogFile, ...
    quotedWrapperPath, additionalSubmitArgs, jobArrayString)
% Create a script that runs the Slurm sbatch command.

% Copyright 2010-2024 The MathWorks, Inc.

if nargin < 6
    jobArrayString = [];
end

dctSchedulerMessage(5, '%s: Creating submit script for %s at %s', mfilename, jobName, outputFilename);

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericSLURM:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
fileCloser = onCleanup(@() fclose(fid));

% Specify shell to use
fprintf(fid, '#!/bin/sh\n');

% Unset all SLURM_ and SBATCH_ variables to avoid conflicting options in
% nested jobs, except for SLURM_CONF which is required for the Slurm
% utilities to work
fprintf(fid, '%s\n', ...
    'for VAR_NAME in $(env | cut -d= -f1 | grep -E ''^(SLURM_|SBATCH_)'' | grep -v ''^SLURM_CONF$''); do', ...
    '    unset "$VAR_NAME"', ...
    'done');

commandToRun = getSubmitString(jobName, quotedLogFile, quotedWrapperPath, ...
    additionalSubmitArgs, jobArrayString);
fprintf(fid, '%s\n', commandToRun);

end
