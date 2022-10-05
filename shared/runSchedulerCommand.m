function [status, result] = runSchedulerCommand(cmd)
%RUNSCHEDULERCOMMAND Make a system call to the scheduler, sanitizing any
% return signals.
%
% Some scheduler utility commands on unix return exit codes > 127, which
% MATLAB interprets as a fatal signal. This is not the case here, so wrap
% the system call to the scheduler on UNIX within a shell script to
% sanitize any exit codes in this range.

% Copyright 2019-2022 The MathWorks, Inc.

persistent wrapper

if isunix
    if isempty(wrapper)
        if verLessThan('matlab', '9.7') % folder renamed in 19b
            dirName = 'distcomp';
        else
            dirName = 'parallel';
        end
        wrapper = fullfile(toolboxdir(dirName), ...
            'bin', 'util', 'shellWrapper.sh'); %#ok<*DCRENAME>
    end
    cmd = sprintf('%s %s', wrapper, cmd);
end

[status, result] = system(cmd);

end
