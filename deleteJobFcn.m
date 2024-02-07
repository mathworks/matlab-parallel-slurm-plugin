function deleteJobFcn(cluster, job)
%DELETEJOBFCN Deletes a job on Slurm
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you delete a job.

% Copyright 2017-2023 The MathWorks, Inc.

cancelJobOnCluster(cluster, job);

end
