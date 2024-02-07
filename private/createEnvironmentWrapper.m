function createEnvironmentWrapper(outputFilename, quotedWrapperPath, environmentVariables)
% Create a script that sets the correct environment variables and then
% calls the job wrapper.

% Copyright 2023 The MathWorks, Inc.

dctSchedulerMessage(5, '%s: Creating environment wrapper at %s', mfilename, outputFilename);

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericSLURM:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
fileCloser = onCleanup(@() fclose(fid));

% Specify shell to use
fprintf(fid, '#!/bin/sh\n');

formatSpec = 'export %s=''%s''\n';

% Write the commands to set and export environment variables
for ii = 1:size(environmentVariables, 1)
    fprintf(fid, formatSpec, environmentVariables{ii,1}, environmentVariables{ii,2});
end

% Write the command to run the job wrapper
fprintf(fid, '%s\n', quotedWrapperPath);

end
