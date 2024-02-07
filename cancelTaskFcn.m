function OK = cancelTaskFcn(cluster, task)
%CANCELTASKFCN Cancels a task on Slurm
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you cancel a task.

% Copyright 2020-2023 The MathWorks, Inc.

OK = cancelTaskOnCluster(cluster, task);

end
