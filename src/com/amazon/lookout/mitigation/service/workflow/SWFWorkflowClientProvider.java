package com.amazon.lookout.mitigation.service.workflow;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Provides the workflow client (decider) to be used when starting a workflow.
 *
 */
public interface SWFWorkflowClientProvider {
    
    // Get workflow client corresponding to the template and device.
    public WorkflowClientExternal getWorkflowClient(String template, String device, long workflowId, TSDMetrics metrics);
}
