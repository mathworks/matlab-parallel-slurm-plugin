function OK = cancelJobOnCluster(cluster, job)
%CANCELJOBONCLUSTER Cancels a job on the Slurm scheduler

% Copyright 2010-2023 The MathWorks, Inc.

% Store the current filename for the errors, warnings and
% dctSchedulerMessages
currFilename = mfilename;
if ~isa(cluster, 'parallel.Cluster')
    error('parallelexamples:GenericSLURM:SubmitFcnError', ...
        'The function %s is for use with clusters created using the parcluster command.', currFilename)
end

% Get the information about the actual cluster used
data = cluster.getJobClusterData(job);
if isempty(data)
    % This indicates that the job has not been submitted, so return true
    dctSchedulerMessage(1, '%s: Job cluster data was empty for job with ID %d.', currFilename, job.ID);
    OK = true;
    return
end

% Get a simplified list of schedulerIDs to reduce the number of calls to
% the scheduler.
schedulerIDs = getSimplifiedSchedulerIDsForJob(job);
erroredJobAndCauseStrings = cell(size(schedulerIDs));
% Get the cluster to delete the job
for ii = 1:length(schedulerIDs)
    schedulerID = schedulerIDs{ii};
    commandToRun = sprintf('scancel -v ''%s''', schedulerID);
    dctSchedulerMessage(4, '%s: Canceling job on cluster using command:\n\t%s.', currFilename, commandToRun);
    try
        [cmdFailed, cmdOut] = runSchedulerCommand(cluster, commandToRun);
    catch err
        cmdFailed = true;
        cmdOut = err.message;
    end
    % scancel can return 0 even if there is an error, so also check the
    % cmdOut does not contain error text. We do not consider attempting
    % to cancel a finished job as a failure, so exclude that.
    if (cmdFailed || contains(cmdOut, 'error:')) && ...
            ~contains(cmdOut, {'already completing', 'Invalid job id specified'})
        % Keep track of all jobs that errored when being cancelled, either
        % through a bad exit code or if an error was thrown. We'll report
        % these later on.
        erroredJobAndCauseStrings{ii} = sprintf('Job ID: %s\tReason: %s', schedulerID, strtrim(cmdOut));
        dctSchedulerMessage(1, '%s: Failed to cancel job %s on cluster.  Reason:\n\t%s', currFilename, schedulerID, cmdOut);
    end
end

if ~cluster.HasSharedFilesystem
    % Only stop mirroring if we are actually mirroring
    remoteConnection = getRemoteConnection(cluster);
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

% Now warn about those jobs that we failed to cancel.
erroredJobAndCauseStrings = erroredJobAndCauseStrings(~cellfun(@isempty, erroredJobAndCauseStrings));
if ~isempty(erroredJobAndCauseStrings)
    warning('parallelexamples:GenericSLURM:FailedToCancelJob', ...
        'Failed to cancel the following jobs on the cluster:\n%s', ...
        sprintf('  %s\n', erroredJobAndCauseStrings{:}));
end
OK = isempty(erroredJobAndCauseStrings);

end
