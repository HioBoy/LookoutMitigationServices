package com.amazon.lookout.mitigation.service.workflow;

import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Provides the workflow client (decider) to be used when starting a workflow.
 *
 */
public interface SWFWorkflowClientProvider {
    
    // Get mitigation modification workflow client for the corresponding template, device and workflowId.
    public WorkflowClientExternal getMitigationModificationWorkflowClient(String template, String device, long workflowId);
    
    // Get reaper workflow client for the corresponding workflowId.
    public WorkflowClientExternal getReaperWorkflowClient(String workflowId);
}
