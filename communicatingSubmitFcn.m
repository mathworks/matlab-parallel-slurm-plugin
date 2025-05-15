function communicatingSubmitFcn(cluster, job, environmentProperties)
%COMMUNICATINGSUBMITFCN Submit a communicating MATLAB job to a Slurm cluster
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you submit a communicating job.
%
% See also parallel.cluster.generic.communicatingDecodeFcn.

% Copyright 2010-2024 The MathWorks, Inc.

% Store the current filename for the errors, warnings and dctSchedulerMessages.
currFilename = mfilename;
if ~isa(cluster, 'parallel.Cluster')
    error('parallelexamples:GenericSLURM:NotClusterObject', ...
        'The function %s is for use with clusters created using the parcluster command.', currFilename)
end

decodeFunction = 'parallel.cluster.generic.communicatingDecodeFcn';

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
    'PARALLEL_SERVER_STORAGE_LOCATION', storageLocation; ...
    'PARALLEL_SERVER_CMR', strip(cluster.ClusterMatlabRoot, 'right', '/'); ...
    'PARALLEL_SERVER_TOTAL_TASKS', num2str(environmentProperties.NumberOfTasks); ...
    'PARALLEL_SERVER_NUM_THREADS', num2str(cluster.NumThreads)};
% Starting in R2025a, IntelMPI is supported via MPIImplementation="IntelMPI"
if ~verLessThan('matlab', '25.1') && ...
        isprop(cluster.AdditionalProperties, 'MPIImplementation') %#ok<VERLESSMATLAB>
    mpiImplementation = cluster.AdditionalProperties.MPIImplementation;
    mustBeMember(mpiImplementation, ["IntelMPI", "MPICH"]);
    variables = [variables; {'PARALLEL_SERVER_MPIEXEC_ARG', ['-', char(mpiImplementation)]}];
end

% Avoid "-bind-to core:N" if AdditionalProperties.UseBindToCore is false (default: true).
if validatedPropValue(cluster.AdditionalProperties, 'UseBindToCore', 'logical', true)
    bindToCoreValue = 'true';
else
    bindToCoreValue = 'false';
end
variables = [variables; {'PARALLEL_SERVER_BIND_TO_CORE', bindToCoreValue}];

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
% List of all the variables to forward through mpiexec to the workers
variables = [variables; ...
    {'PARALLEL_SERVER_GENVLIST', strjoin(variables(:,1), ',')}];

% The job directory as accessed by this machine
localJobDirectory = cluster.getJobFolder(job);

% The job directory as accessed by workers on the cluster
if cluster.HasSharedFilesystem
    jobDirectoryOnCluster = cluster.getJobFolderOnCluster(job);
else
    jobDirectoryOnCluster = remoteConnection.getRemoteJobLocation(job.ID, clusterOS);
end

% Specify the job wrapper script to use.
% Prior to R2019a, only the SMPD process manager is supported.
if verLessThan('matlab', '9.6') || ...
        validatedPropValue(cluster.AdditionalProperties, 'UseSmpd', 'logical', false)
    if ~verLessThan('matlab', '25.1') %#ok<VERLESSMATLAB>
        % Starting in R2025a, smpd launcher is not supported.
        error('parallelexamples:GenericSLURM:SmpdNoLongerSupported', ...
            'The smpd process manager is no longer supported.');
    end
    jobWrapperName = 'communicatingJobWrapperSmpd.sh';
else
    jobWrapperName = 'communicatingJobWrapper.sh';
end
% The wrapper script is in the same directory as this file
dirpart = fileparts(mfilename('fullpath'));
localScript = fullfile(dirpart, jobWrapperName);
% Copy the local wrapper script to the job directory
copyfile(localScript, localJobDirectory, 'f');

% The script to execute on the cluster to run the job
wrapperPath = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, jobWrapperName);
quotedWrapperPath = sprintf('%s%s%s', quote, wrapperPath, quote);

% Choose a file for the output
logFile = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, sprintf('Job%d.log', job.ID));
quotedLogFile = sprintf('%s%s%s', quote, logFile, quote);
dctSchedulerMessage(5, '%s: Using %s as log file', currFilename, quotedLogFile);

jobName = sprintf('MATLAB_R%s_Job%d', version('-release'), job.ID);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CUSTOMIZATION MAY BE REQUIRED %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% You might want to customize this section to match your cluster,
% for example to limit the number of nodes for a single job.
additionalSubmitArgs = sprintf('--ntasks=%d --cpus-per-task=%d', environmentProperties.NumberOfTasks, cluster.NumThreads);
commonSubmitArgs = getCommonSubmitArgs(cluster);
additionalSubmitArgs = strtrim(sprintf('%s %s', additionalSubmitArgs, commonSubmitArgs));
if validatedPropValue(cluster.AdditionalProperties, 'DisplaySubmitArgs', 'logical', false)
    fprintf('Submit arguments: %s\n', additionalSubmitArgs);
end

% Path to the submit script, to submit the Slurm job using sbatch
submitScriptName = sprintf('submitScript%s', scriptExt);
localSubmitScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, submitScriptName);
submitScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, submitScriptName);
quotedSubmitScriptPathOnCluster = sprintf('%s%s%s', quote, submitScriptPathOnCluster, quote);

% Path to the environment wrapper, which will set the environment variables
% for the job then execute the job wrapper
envScriptName = sprintf('environmentWrapper%s', scriptExt);
localEnvScriptPath = sprintf('%s%s%s', localJobDirectory, fileSeparator, envScriptName);
envScriptPathOnCluster = sprintf('%s%s%s', jobDirectoryOnCluster, fileSeparator, envScriptName);
quotedEnvScriptPathOnCluster = sprintf('%s%s%s', quote, envScriptPathOnCluster, quote);

% Create the scripts to submit a Slurm job.
% These will be created in the job directory.
dctSchedulerMessage(5, '%s: Generating scripts for job %d', currFilename, job.ID);
createEnvironmentWrapper(localEnvScriptPath, quotedWrapperPath, variables);
createSubmitScript(localSubmitScriptPath, jobName, quotedLogFile, ...
    quotedEnvScriptPathOnCluster, additionalSubmitArgs);

% Create the command to run on the cluster
commandToRun = sprintf('%s %s', shellCmd, quotedSubmitScriptPathOnCluster);

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

% Now ask the cluster to run the submission command
dctSchedulerMessage(4, '%s: Submitting job using command:\n\t%s', currFilename, commandToRun);
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

% Calculate the schedulerIDs
jobIDs = extractJobId(cmdOut);
if isempty(jobIDs)
    error('parallelexamples:GenericSLURM:FailedToParseSubmissionOutput', ...
        'Failed to parse the job identifier from the submission output: "%s"', ...
        cmdOut);
end
% jobIDs must be a cell array
if ~iscell(jobIDs)
    jobIDs = {jobIDs};
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
    jobData.ClusterJobIDs = jobIDs;
else % schedulerID on task since 19b
    if isscalar(job.Tasks)
        schedulerIDs = jobIDs{1};
    else
        schedulerIDs = repmat(jobIDs, size(job.Tasks));
    end
    set(job.Tasks, 'SchedulerID', schedulerIDs);
end
cluster.setJobClusterData(job, jobData);

end
