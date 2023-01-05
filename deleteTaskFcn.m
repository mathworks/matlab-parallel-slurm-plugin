function deleteTaskFcn(cluster, task)
%DELETEJOBFCN Deletes a job on cluster
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you delete a job.

% Copyright 2020-2022 The MathWorks, Inc.

cancelTaskFcn(cluster, task);

end
