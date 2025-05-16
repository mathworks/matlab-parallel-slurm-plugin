function independentSubmitFcn(cluster, job, environmentProperties)
%INDEPENDENTSUBMITFCN Submit a MATLAB job to a Slurm cluster
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you submit an independent job.
%
% See also parallel.cluster.generic.independentDecodeFcn.

% Copyright 2010-2024 The MathWorks, Inc.

% Store the current filename for the errors, warnings and dctSchedulerMessages.
currFilename = mfilename;
if ~isa(cluster, 'parallel.Cluster')
    error('parallelexamples:GenericSLURM:NotClusterObject', ...
        'The function %s is for use with clusters created using the parcluster command.', currFilename)
end

decodeFunction = 'parallel.cluster.generic.independentDecodeFcn';

clusterOS = cluster.OperatingSystem;
if ~strcmpi(clusterOS, 'unix')
    error('parallelexamples:GenericSLURM:UnsupportedOS', ...
        'The function %s only supports clusters with the unix operating system.', currFilename)
end

% Get the correct quote and file separator for the Cluster OS.
% This check is unnecessary in this file because we explicitly
% checked that the clusterOS is unix. This code is an example
% of how to deal with clusters that can be unix or pc.
if strcmpi(clusterOS, 'unix')
    quote = '''';
    fileSeparator = '/';
    scriptExt = '.sh';
    shellCmd = 'sh';
else
    quote = '"';
    fileSeparator = '\';
    scriptExt = '.bat';
    shellCmd = 'cmd /c';
end

if isprop(cluster.AdditionalProperties, 'ClusterHost')
    remoteConnection = getRemoteConnection(cluster);
end

[useJobArrays, maxJobArraySize] = iGetJobArrayProps(cluster);
% Store data for future reference
cluster.UserData.UseJobArrays = useJobArrays;
if useJobArrays
    cluster.UserData.MaxJobArraySize = maxJobArraySize;
end

% Determine the debug setting. Setting to true makes the MATLAB workers
% output additional logging. If EnableDebug is set in the cluster object's
% AdditionalProperties, that takes precedence. Otherwise, look for the
% PARALLEL_SERVER_DEBUG and MDCE_DEBUG environment variables in that order.
% If nothing is set, debug is false.
enableDebug = 'false';
if isprop(cluster.AdditionalProperties, 'EnableDebug')
    % Use AdditionalProperties.EnableDebug, if it is set
    enableDebug = char(string(cluster.AdditionalProperties.EnableDebug));
else
    % Otherwise check the environment variables set locally on the client
    environmentVariablesToCheck = {'PARALLEL_SERVER_DEBUG', 'MDCE_DEBUG'};
    for idx = 1:numel(environmentVariablesToCheck)
        debugValue = getenv(environmentVariablesToCheck{idx});
        if ~isempty(debugValue)
            enableDebug = debugValue;
            break
        end
    end
end

% The job specific environment variables
% Remove leading and trailing whitespace from the MATLAB arguments
matlabArguments = strtrim(environmentProperties.MatlabArguments);

% Where the workers store job output
if cluster.HasSharedFilesystem
    storageLocation = environmentProperties.StorageLocation;
else
    storageLocation = remoteConnection.JobStorageLocation;
    % If the RemoteJobStorageLocation ends with a space, add a slash to ensure it is respected
    if endsWith(storageLocation, ' ')
        storageLocation = [storageLocation, fileSeparator];
    end
end
variables = { ...
    'PARALLEL_SERVER_DECODE_FUNCTION', decodeFunction; ...
    'PARALLEL_SERVER_STORAGE_CONSTRUCTOR', environmentProperties.StorageConstructor; ...
    'PARALLEL_SERVER_JOB_LOCATION', environmentProperties.JobLocation; ...
    'PARALLEL_SERVER_MATLAB_EXE', environmentProperties.MatlabExecutable; ...
    'PARALLEL_SERVER_MATLAB_ARGS', matlabArguments; ...
    'PARALLEL_SERVER_DEBUG', enableDebug; ...
    'MLM_WEB_LICENSE', environmentProperties.UseMathworksHostedLicensing; ...
    'MLM_WEB_USER_CRED', environmentProperties.UserToken; ...
    'MLM_WEB_ID', environmentProperties.LicenseWebID; ...
    'PARALLEL_SERVER_LICENSE_NUMBER', environmentProperties.LicenseNumber; ...
    'PARALLEL_SERVER_STORAGE_LOCATION', storageLocation};
if ~verLessThan('matlab', '25.1') %#ok<VERLESSMATLAB>
    variables = [variables; environmentProperties.JobEnvironment];
end
% Environment variable names different prior to 19b
if verLessThan('matlab', '9.7')
    variables(:,1) = replace(variables(:,1), 'PARALLEL_SERVER_', 'MDCE_');
end
% Trim the environment variables of empty values.
nonEmptyValues = cellfun(@(x) ~isempty(strtrim(x)), variables(:,2));
variables = variables(nonEmptyValues, :);

% The job directory as accessed by this machine
localJobDirectory = cluster.getJobFolder(job);

% The job directory as accessed by workers on the cluster
if cluster.HasSharedFilesystem
    jobDirectoryOnCluster = cluster.getJobFolderOnCluster(job);
else
    jobDirectoryOnCluster = remoteConnection.getRemoteJobLocation(job.ID, clusterOS);
end

% Name of the wrapper script to launch the MATLAB worker
jobWrapperName = 'independentJobWrapper.sh';
% The wrapper script is in the same directory as this file
dirpart = fileparts(mfilename('fullpath'));
localScript = fullfile(dirpart, jobWrapperName);
% Copy the local wrapper script to the job directory
copyfile(localScript, localJobDirectory, 'f');

% The script to execute on the cluster to run the job
wrapperPath = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, jobWrapperName);
quotedWrapperPath = sprintf('%s%s%s', quote, wrapperPath, quote);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CUSTOMIZATION MAY BE REQUIRED %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
additionalSubmitArgs = sprintf('--ntasks=1 --cpus-per-task=%d', cluster.NumThreads);
commonSubmitArgs = getCommonSubmitArgs(cluster);
additionalSubmitArgs = strtrim(sprintf('%s %s', additionalSubmitArgs, commonSubmitArgs));
if validatedPropValue(cluster.AdditionalProperties, 'DisplaySubmitArgs', 'logical', false)
    fprintf('Submit arguments: %s\n', additionalSubmitArgs);
end

% Only keep and submit tasks that are not cancelled. Cancelled tasks
% will have errors.
isPendingTask = cellfun(@isempty, get(job.Tasks, {'Error'}));
tasks = job.Tasks(isPendingTask);
taskIDs = cell2mat(get(tasks, {'ID'}));
numberOfTasks = numel(tasks);

% Only use job arrays when you can get enough use out of them.
% The submission method in this function requires a minimum maxJobArraySize
% of 10 to get enough use of job arrays.
if numberOfTasks < 2 || maxJobArraySize < 10
    useJobArrays = false;
end

if useJobArrays
    % Check if there are more tasks than will fit in one job array. Slurm
    % will not accept a job array index greater than its MaxArraySize
    % parameter, as defined in slurm.conf, even if the overall size of the
    % array is less than MaxArraySize. For example, for the default
    % (inclusive) upper limit of MaxArraySize=1000, array indices of 1 to
    % 1000 would be accepted, but 1001 or above would not. To get around
    % this restriction, submit the full array of tasks in multiple Slurm
    % job arrays, hereafter referred to as subarrays. Round the
    % MaxArraySize down to the nearest power of 10, as this allows the log
    % file of taskX to be named TaskX.log.  See iGenerateLogFileName.
    if taskIDs(end) > maxJobArraySize
        % Use the nearest power of 10 as subarray size. This will make the
        % naming of log files easier.
        maxJobArraySizeToUse = 10^floor(log10(maxJobArraySize));
        % Group task IDs into bins of jobArraySize size.
        groups = findgroups(floor(taskIDs./maxJobArraySizeToUse));
        % Count the number of elements in each group and form subarrays.
        jobArraySizes = splitapply(@numel, taskIDs, groups);
    else
        maxJobArraySizeToUse = maxJobArraySize;
        jobArraySizes = numel(tasks);
    end
    taskIDGroupsForJobArrays = mat2cell(taskIDs,jobArraySizes);
    
    jobName = sprintf('MATLAB_R%s_Job%d', version('-release'), job.ID);
    numJobArrays = numel(taskIDGroupsForJobArrays);
    commandsToRun = cell(numJobArrays, 1);
    jobIDs = cell(numJobArrays, 1);
    schedulerJobArrayIndices = cell(numJobArrays, 1);
    for ii = 1:numJobArrays
        % Slurm only accepts task IDs up to maxArraySize. Shift all task
        % IDs down below the limit.
        taskOffset = (ii-1)*maxJobArraySizeToUse;
        schedulerJobArrayIndices{ii} = taskIDGroupsForJobArrays{ii} - taskOffset;
        % Save the offset as an environment variable to pass to the tasks
        % during Slurm submission.
        environmentVariables = [variables; ...
            {'PARALLEL_SERVER_TASK_ID_OFFSET', num2str(taskOffset)}];
        
        % Create a character vector with the ranges of IDs to submit.
        jobArrayString = iCreateJobArrayString(schedulerJobArrayIndices{ii});
        
        % Choose a file for the output
        logFileName = iGenerateLogFileName(ii, maxJobArraySizeToUse);
        logFile = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, logFileName);
        quotedLogFile = sprintf('%s%s%s', quote, logFile, quote);
        dctSchedulerMessage(5, '%s: Using %s as log file', currFilename, quotedLogFile);
        
        % Path to the submit script, to submit the Slurm job using sbatch
        submitScriptName = sprintf('submitScript%d%s', ii, scriptExt);
        localSubmitScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, submitScriptName);
        submitScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, submitScriptName);
        quotedSubmitScriptPathOnCluster = sprintf('%s%s%s', quote, submitScriptPathOnCluster, quote);
        
        % Path to the environment wrapper, which will set the environment variables
        % for the job then execute the job wrapper
        envScriptName = sprintf('environmentWrapper%d%s', ii, scriptExt);
        localEnvScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, envScriptName);
        envScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, envScriptName);
        quotedEnvScriptPathOnCluster = sprintf('%s%s%s', quote, envScriptPathOnCluster, quote);
        
        % Create the scripts to submit a Slurm job.
        % These will be created in the job directory.
        dctSchedulerMessage(5, '%s: Generating scripts for job array %d', currFilename, ii);
        createEnvironmentWrapper(localEnvScriptPath, quotedWrapperPath, environmentVariables);
        createSubmitScript(localSubmitScriptPath, jobName, quotedLogFile, ...
            quotedEnvScriptPathOnCluster, additionalSubmitArgs, jobArrayString);
        
        % Create the command to run on the cluster
        commandsToRun{ii} = sprintf('%s %s', shellCmd, quotedSubmitScriptPathOnCluster);
    end
else
    % Do not use job arrays and submit each task individually.
    taskLocations = environmentProperties.TaskLocations(isPendingTask);
    jobIDs = cell(1, numberOfTasks);
    commandsToRun = cell(numberOfTasks, 1);
    
    % Loop over every task we have been asked to submit
    for ii = 1:numberOfTasks
        taskLocation = taskLocations{ii};
        % Add the task location to the environment variables
        if verLessThan('matlab', '9.7') % variable name changed in 19b
            environmentVariables = [variables; ...
                {'MDCE_TASK_LOCATION', taskLocation}];
        else
            environmentVariables = [variables; ...
                {'PARALLEL_SERVER_TASK_LOCATION', taskLocation}];
        end
        
        % Choose a file for the output
        logFileName = sprintf('Task%d.log', taskIDs(ii));
        logFile = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, logFileName);
        quotedLogFile = sprintf('%s%s%s', quote, logFile, quote);
        dctSchedulerMessage(5, '%s: Using %s as log file', currFilename, quotedLogFile);
        
        % Submit one task at a time
        jobName = sprintf('MATLAB_R%s_Job%d.%d', version('-release'), job.ID, taskIDs(ii));
        
        % Path to the submit script, to submit the Slurm job using sbatch
        submitScriptName = sprintf('submitScript%d%s', ii, scriptExt);
        localSubmitScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, submitScriptName);
        submitScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, submitScriptName);
        quotedSubmitScriptPathOnCluster = sprintf('%s%s%s', quote, submitScriptPathOnCluster, quote);
        
        % Path to the environment wrapper, which will set the environment variables
        % for the job then execute the job wrapper
        envScriptName = sprintf('environmentWrapper%d%s', ii, scriptExt);
        localEnvScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, envScriptName);
        envScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, envScriptName);
        quotedEnvScriptPathOnCluster = sprintf('%s%s%s', quote, envScriptPathOnCluster, quote);
        
        % Create the scripts to submit a Slurm job.
        % These will be created in the job directory.
        dctSchedulerMessage(5, '%s: Generating scripts for task %d', currFilename, ii);
        createEnvironmentWrapper(localEnvScriptPath, quotedWrapperPath, environmentVariables);
        createSubmitScript(localSubmitScriptPath, jobName, quotedLogFile, ...
            quotedEnvScriptPathOnCluster, additionalSubmitArgs);
        
        % Create the command to run on the cluster
        commandsToRun{ii} = sprintf('%s %s', shellCmd, quotedSubmitScriptPathOnCluster);
    end
end

if ~cluster.HasSharedFilesystem
    % Start the mirror to copy all the job files over to the cluster
    dctSchedulerMessage(4, '%s: Starting mirror for job %d.', currFilename, job.ID);
    remoteConnection.startMirrorForJob(job);
end

if strcmpi(clusterOS, 'unix')
    % Add execute permissions to shell scripts
    runSchedulerCommand(cluster, sprintf( ...
        'chmod u+x "%s%s"*.sh', jobDirectoryOnCluster, fileSeparator));
    % Convert line endings to Unix
    runSchedulerCommand(cluster, sprintf( ...
        'dos2unix --allow-chown "%s%s"*.sh', jobDirectoryOnCluster, fileSeparator));
end

for ii=1:numel(commandsToRun)
    commandToRun = commandsToRun{ii};
    jobIDs{ii} = iSubmitJobUsingCommand(cluster, job, commandToRun);
end

% Calculate the schedulerIDs
if useJobArrays
    % The scheduler ID of each task is a combination of the job ID and the
    % scheduler array index. cellfun pairs each job ID with its
    % corresponding scheduler array indices in schedulerJobArrayIndices and
    % returns the combination of both. For example, if jobIDs = {1,2} and
    % schedulerJobArrayIndices = {[1,2];[3,4]}, the schedulerID is given by
    % combining 1 with [1,2] and 2 with [3,4], in the canonical form of the
    % scheduler.
    schedulerIDs = cellfun(@(jobID,arrayIndices) jobID + "_" + arrayIndices, ...
        jobIDs, schedulerJobArrayIndices, 'UniformOutput',false);
    schedulerIDs = vertcat(schedulerIDs{:});
else
    % The scheduler ID of each task is the job ID.
    schedulerIDs = string(jobIDs);
end

% Store the scheduler ID for each task and the job cluster data
jobData = struct('type', 'generic');
if isprop(cluster.AdditionalProperties, 'ClusterHost')
    % Store the cluster host
    jobData.RemoteHost = remoteConnection.Hostname;
end
if ~cluster.HasSharedFilesystem
    % Store the remote job storage location
    jobData.RemoteJobStorageLocation = remoteConnection.JobStorageLocation;
    jobData.HasDoneLastMirror = false;
end
if verLessThan('matlab', '9.7') % schedulerID stored in job data
    jobData.ClusterJobIDs = schedulerIDs;
else % schedulerID on task since 19b
    set(tasks, 'SchedulerID', schedulerIDs);
end
cluster.setJobClusterData(job, jobData);

end

function [useJobArrays, maxJobArraySize] = iGetJobArrayProps(cluster)
% Look for useJobArrays and maxJobArray size in the following order:
% 1.  Additional Properties
% 2.  User Data
% 3.  Query scheduler for MaxJobArraySize

useJobArrays = validatedPropValue(cluster.AdditionalProperties, 'UseJobArrays', 'logical');
if isempty(useJobArrays)
    if isfield(cluster.UserData, 'UseJobArrays')
        useJobArrays = cluster.UserData.UseJobArrays;
    else
        useJobArrays = true;
    end
end

if ~useJobArrays
    % Not using job arrays so don't need the max array size
    maxJobArraySize = 0;
    return
end

maxJobArraySize = validatedPropValue(cluster.AdditionalProperties, 'MaxJobArraySize', 'numeric');
if ~isempty(maxJobArraySize)
    if maxJobArraySize < 1
        error('parallelexamples:GenericSLURM:IncorrectArguments', ...
            'MaxJobArraySize must be a positive integer');
    end
    return
end

if isfield(cluster.UserData,'MaxJobArraySize')
    maxJobArraySize = cluster.UserData.MaxJobArraySize;
    return
end

% Get job array information by querying the scheduler.
commandToRun = 'scontrol show config';
try
    [cmdFailed, cmdOut] = runSchedulerCommand(cluster, commandToRun);
catch err
    cmdFailed = true;
    cmdOut = err.message;
end
if cmdFailed
    error('parallelexamples:GenericSLURM:FailedToRetrieveInfo', ...
        'Failed to retrieve Slurm configuration information using command:\n\t%s.\nReason: %s', ...
        commandToRun, cmdOut);
end

maxJobArraySize = 0;
% Extract the maximum array size for job arrays. For Slurm, the
% configuration line that contains the maximum array index looks like this:
% MaxArraySize = 1000
% Use a regular expression to extract this parameter.
tokens = regexp(cmdOut,'MaxArraySize\s*=\s*(\d+)', 'tokens','once');

if isempty(tokens) || (str2double(tokens) == 0)
    % No job array support.
    useJobArrays = false;
    return
end

useJobArrays = true;
% Set the maximum array size.
maxJobArraySize = str2double(tokens{1});
% In Slurm, MaxArraySize is an exclusive upper bound. Subtract one to obtain
% the inclusive upper bound.
maxJobArraySize = maxJobArraySize - 1;
end

function jobID = iSubmitJobUsingCommand(cluster, job, commandToRun)
currFilename = mfilename;
% Ask the cluster to run the submission command.
dctSchedulerMessage(4, '%s: Submitting job %d using command:\n\t%s', currFilename, job.ID, commandToRun);
try
    [cmdFailed, cmdOut] = runSchedulerCommand(cluster, commandToRun);
catch err
    cmdFailed = true;
    cmdOut = err.message;
end
if cmdFailed
    if ~cluster.HasSharedFilesystem
        % Stop the mirroring if we failed to submit the job - this will also
        % remove the job files from the remote location
        remoteConnection = getRemoteConnection(cluster);
        % Only stop mirroring if we are actually mirroring
        if remoteConnection.isJobUsingConnection(job.ID)
            dctSchedulerMessage(5, '%s: Stopping the mirror for job %d.', currFilename, job.ID);
            try
                remoteConnection.stopMirrorForJob(job);
            catch err
                warning('parallelexamples:GenericSLURM:FailedToStopMirrorForJob', ...
                    'Failed to stop the file mirroring for job %d.\nReason: %s', ...
                    job.ID, err.getReport);
            end
        end
    end
    error('parallelexamples:GenericSLURM:FailedToSubmitJob', ...
        'Failed to submit job to Slurm using command:\n\t%s.\nReason: %s', ...
        commandToRun, cmdOut);
end

jobID = extractJobId(cmdOut);
if isempty(jobID)
    error('parallelexamples:GenericSLURM:FailedToParseSubmissionOutput', ...
        'Failed to parse the job identifier from the submission output: "%s"', ...
        cmdOut);
end
end

function rangesString = iCreateJobArrayString(taskIDs)
% Create a character vector with the ranges of task IDs to submit
if taskIDs(end) - taskIDs(1) + 1 == numel(taskIDs)
    % There is only one range.
    rangesString = sprintf('%d-%d',taskIDs(1),taskIDs(end));
else
    % There are several ranges.
    % Calculate the step size between task IDs.
    step = diff(taskIDs);
    % Where the step changes, a range ends and another starts. Include
    % the initial and ending IDs in the ranges as well.
    isStartOfRange = [true; step > 1];
    isEndOfRange   = [step > 1; true];
    rangesString = strjoin(compose('%d-%d', ...
        taskIDs(isStartOfRange),taskIDs(isEndOfRange)),',');
end
end

function logFileName = iGenerateLogFileName(subArrayIdx, jobArraySize)
% This function builds the log file specifier, which is then passed to
% Slurm to tell it where each task's output should go. This will be equal
% to TaskX.log where X is the MATLAB ID. Slurm will not accept a job array
% index greater than its MaxArraySize parameter. As a result MATLAB IDs
% must be shifted down below MaxArraySize. To ensure that the log file for
% Task X is called TaskX.log, round the maximum array size down to the
% nearest power of 10 and manually construct the log file specifier. For
% example, for a MaxArraySize of 1500, the Slurm job arrays will be of
% size 1000, and MATLAB task IDs will map as illustrated by the following
% table:
%
%    MATLAB ID | Slurm ID | Log file specifier
%    ----------+----------+--------------------
%       1- 999 |   1-999  | Task%a.log
%    1000-1999 | 000-999  | Task1%3a.log
%    2000-2999 | 000-999  | Task2%3a.log
%    3000      | 000      | Task3%3a.log
%
% Note that Slurm expands %a to the Slurm ID, and %3a to the Slurm ID
% padded with zeros to 3 digits.
if subArrayIdx == 1
    % Job arrays have more than one task. Use %a so that Slurm expands it
    % into the actual task ID.
    logFileName = 'Task%a.log';
else
    % For subsequent subarrays after the first one, prepend the index to %a
    % to identify the batch of log files and form the final log file name.
    padding = floor(log10(jobArraySize));
    logFileName = sprintf('Task%d%%%da.log',subArrayIdx-1,padding);
end
end
