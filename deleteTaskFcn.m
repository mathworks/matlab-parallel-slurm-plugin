function deleteTaskFcn(cluster, task)
%DELETETASKFCN Deletes a job on Slurm
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you delete a job.

% Copyright 2020-2023 The MathWorks, Inc.

cancelTaskOnCluster(cluster, task);

end
