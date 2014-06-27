package com.amazon.lookout.mitigation.service.workflow;

import java.util.Set;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Interface for classes that help the LookoutMitigationService Activity to start a new workflow.
 */
public interface SWFWorkflowStarter {
    
    // Create a new workflow client which will be used to run the new workflow.
    public WorkflowClientExternal createSWFWorkflowClient(long workflowId, MitigationModificationRequest request, String deviceName, TSDMetrics metrics);
    
    // Start a new workflow based on the input parameters.
    public void startWorkflow(long workflowId, MitigationModificationRequest request, Set<String> locations, RequestType requestType, int mitigationVersion, 
    		                  String deviceName, String deviceScope, WorkflowClientExternal workflowExternalClient, TSDMetrics metrics);
}