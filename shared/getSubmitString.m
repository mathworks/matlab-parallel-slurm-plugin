function submitString = getSubmitString(jobName, quotedLogFile, quotedCommand, ...
    varsToForward, additionalSubmitArgs, jobArrayString)
%GETSUBMITSTRING Gets the correct sbatch command for a Slurm cluster

% Copyright 2010-2022 The MathWorks, Inc.

envString = strjoin(varsToForward, ',');

if ~isempty(jobArrayString)
    jobArrayString = strcat('--array=''[', jobArrayString, ']''');
end

submitString = sprintf('sbatch --job-name=%s %s --output=%s --export=%s %s %s', ...
    jobName, jobArrayString, quotedLogFile, envString, additionalSubmitArgs, quotedCommand);

end
