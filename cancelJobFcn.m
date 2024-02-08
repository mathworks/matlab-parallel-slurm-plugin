function OK = cancelJobFcn(cluster, job)
%CANCELJOBFCN Cancels a job on Slurm
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you cancel a job.

% Copyright 2010-2023 The MathWorks, Inc.

OK = cancelJobOnCluster(cluster, job);

end
