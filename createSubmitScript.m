function createSubmitScript(outputFilename, jobName, quotedLogFile, ...
    quotedWrapperPath, additionalSubmitArgs, jobArrayString)
% Create a script that runs the Slurm sbatch command.

% Copyright 2010-2023 The MathWorks, Inc.

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

commandToRun = getSubmitString(jobName, quotedLogFile, quotedWrapperPath, ...
    additionalSubmitArgs, jobArrayString);
fprintf(fid, '%s\n', commandToRun);

end
