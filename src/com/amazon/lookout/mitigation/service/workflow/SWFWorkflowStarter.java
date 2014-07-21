package com.amazon.lookout.mitigation.service.workflow;

import java.util.Set;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.workflow.model.RequestToReap;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

/**
 * Interface for classes that help the LookoutMitigationService Activity to start a new workflow.
 */
public interface SWFWorkflowStarter {
    
    // Create a new workflow client which will be used to run the new mitigation modification workflow.
    public WorkflowClientExternal createMitigationModificationWorkflowClient(long workflowId, MitigationModificationRequest request, String deviceName, TSDMetrics metrics);
    
    // Start a new mitigation modification workflow based on the input parameters.
    public void startMitigationModificationWorkflow(long workflowId, MitigationModificationRequest request, Set<String> locations, RequestType requestType, int mitigationVersion, 
                                                    String deviceName, String deviceScope, WorkflowClientExternal workflowExternalClient, TSDMetrics metrics);
    
    public WorkflowClientExternal createReaperWorkflowClient(String workflowId, TSDMetrics metrics);
    
    public void startReaperWorkflow(String workflowId, RequestToReap requestToReap, WorkflowClientExternal workflowExternalClient, TSDMetrics metrics);
}
