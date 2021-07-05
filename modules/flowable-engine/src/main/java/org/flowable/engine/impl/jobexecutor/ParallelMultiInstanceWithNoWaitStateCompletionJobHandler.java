/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flowable.engine.impl.jobexecutor;

import org.flowable.bpmn.model.Activity;
import org.flowable.bpmn.model.FlowElement;
import org.flowable.common.engine.impl.interceptor.CommandContext;
import org.flowable.engine.delegate.DelegateExecution;
import org.flowable.engine.impl.bpmn.behavior.ParallelMultiInstanceBehavior;
import org.flowable.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.flowable.engine.impl.persistence.entity.ExecutionEntity;
import org.flowable.engine.impl.persistence.entity.ExecutionEntityManager;
import org.flowable.engine.impl.util.CommandContextUtil;
import org.flowable.engine.impl.util.ExecutionGraphUtil;
import org.flowable.engine.impl.util.JobUtil;
import org.flowable.job.service.JobHandler;
import org.flowable.job.service.JobService;
import org.flowable.job.service.impl.persistence.entity.JobEntity;
import org.flowable.variable.api.delegate.VariableScope;

/**
 * @author Joram Barrez
 */
public class ParallelMultiInstanceWithNoWaitStateCompletionJobHandler implements JobHandler {

    public static final String TYPE = "parallel-multi-instance-no-wait-state-complete";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void execute(JobEntity job, String configuration, VariableScope variableScope, CommandContext commandContext) {
        ProcessEngineConfigurationImpl processEngineConfiguration = CommandContextUtil.getProcessEngineConfiguration(commandContext);
        ExecutionEntityManager executionEntityManager = processEngineConfiguration.getExecutionEntityManager();

        ExecutionEntity execution = executionEntityManager.findById(job.getExecutionId());
        if (execution != null) {
            FlowElement currentFlowElement = execution.getCurrentFlowElement();
            if (currentFlowElement instanceof Activity) {
                Object behavior = ((Activity) currentFlowElement).getBehavior();
                if (behavior instanceof ParallelMultiInstanceBehavior) {
                    ParallelMultiInstanceBehavior parallelMultiInstanceBehavior = (ParallelMultiInstanceBehavior) behavior;

                    DelegateExecution multiInstanceRootExecution = ExecutionGraphUtil.getMultiInstanceRootExecution(execution);
                    if (multiInstanceRootExecution != null) {

                        // Optimization to do the active count here: if there are still active executions in the database, there's no need to do anything:
                        // no need to fetch any variables such as the completed, nr of active, etc.
                        // The job can simply be rescheduled and it will try the same logic again later as things are not yet done.
                        long activeChildExecutionCount = executionEntityManager.countActiveExecutionsByParentId(multiInstanceRootExecution.getId());
                        if (activeChildExecutionCount > 0) {
                            reCreateJob(processEngineConfiguration, execution);

                        } else {
                            boolean multiInstanceCompleted = parallelMultiInstanceBehavior.leaveAsync(execution);
                            if (!multiInstanceCompleted) {
                                reCreateJob(processEngineConfiguration, execution);
                            }

                        }
                    }
                }
            }
        }
    }

    protected void reCreateJob(ProcessEngineConfigurationImpl processEngineConfiguration, ExecutionEntity execution) {
        // Not the usual way of creating a job, as we specifically don't want the async executor to trigger.
        // The job should be picked up in the next acquire cycle, as to not continuous loop.
        JobService jobService = processEngineConfiguration.getJobServiceConfiguration().getJobService();
        JobEntity newJob = JobUtil.createJob(execution, TYPE, processEngineConfiguration);
        jobService.createAsyncJobNoTriggerAsyncExecutor(newJob, true);
        jobService.insertJob(newJob);
    }

}