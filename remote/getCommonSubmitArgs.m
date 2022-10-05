function commonSubmitArgs = getCommonSubmitArgs(cluster)
% Get any additional submit arguments for the Slurm sbatch command
% that are common to both independent and communicating jobs.

% Copyright 2016-2022 The MathWorks, Inc.

commonSubmitArgs = '';
ap = cluster.AdditionalProperties;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CUSTOMIZATION MAY BE REQUIRED %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% You may wish to support further cluster.AdditionalProperties fields here
% and modify the submission command arguments accordingly.

% Account name
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'AccountName', 'char', '-A %s');

% Constraint
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'Constraint', 'char', '-C %s');

% Memory required per CPU
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'MemPerCPU', 'char', '--mem-per-cpu=%s');

% Partition (queue)
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'Partition', 'char', '-p %s');

% Require exclusive use of requested nodes
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'RequireExclusiveNode', 'logical', '--exclusive');

% Reservation
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'Reservation', 'char', '--reservation=%s');

% Wall time
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'WallTime', 'char', '-t %s');

% Email notification
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'EmailAddress', 'char', '--mail-type=ALL --mail-user=%s');

% Catch all: directly append anything in the AdditionalSubmitArgs
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'AdditionalSubmitArgs', 'char', '%s');

% Trim any whitespace
commonSubmitArgs = strtrim(commonSubmitArgs);

end

function commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, propName, propType, submitPattern)
arg = validatedPropValue(ap, propName, propType);
if ~isempty(arg) && (~islogical(arg) || arg)
    commonSubmitArgs = sprintf([commonSubmitArgs ' ' submitPattern], arg);
end
end
