function [status, result] = runSchedulerCommand(cluster, cmd)
%RUNSCHEDULERCOMMAND Run a command on the cluster.

% Copyright 2019-2024 The MathWorks, Inc.

persistent wrapper

if isprop(cluster.AdditionalProperties, 'ClusterHost')
    % Need to run the command over SSH
    remoteConnection = getRemoteConnection(cluster);
    [status, result] = remoteConnection.runCommand(cmd);
else
    % Can shell out
    if isunix
        % Some scheduler utility commands on unix return exit codes > 127, which
        % MATLAB interprets as a fatal signal. This is not the case here, so wrap
        % the system call to the scheduler on UNIX within a shell script to
        % sanitize any exit codes in this range.
        if isempty(wrapper)
            wrapper = iBuildWrapperPath();
        end
        cmd = sprintf('%s %s', wrapper, cmd);
    end
    [status, result] = system(cmd);
end

end

function wrapper = iBuildWrapperPath()
if verLessThan('matlab', '9.7')
    pctDir = toolboxdir('distcomp'); %#ok<*DCRENAME>
elseif verLessThan('matlab', '25.1') %#ok<*VERLESSMATLAB>
    pctDir = toolboxdir('parallel');
else
    pctDir = fullfile(matlabroot, 'toolbox', 'parallel');
end
wrapper = fullfile(pctDir, 'bin', 'util', 'shellWrapper.sh');
end
