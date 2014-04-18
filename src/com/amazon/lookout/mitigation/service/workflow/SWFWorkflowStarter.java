package com.amazon.lookout.mitigation.service.workflow;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * Interface for classes that help the LookoutMitigationService Activity to start a new workflow.
 */
public interface SWFWorkflowStarter {
    
    // Start a new workflow based on the input parameters.
    public void startWorkflow(long workflowId, MitigationModificationRequest request, int mitigationVersion, String deviceName, TSDMetrics metrics);
}
