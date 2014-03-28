package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;

/**
 * WorkflowIdGenerator has the responsibility of generating new workflowIds when clients call generateWorkflowId().
 * 
 * When clients have successfully acquired the new workflowId, they should be able to call confirmAcquiringWorkflowId() for the
 * WorkflowIdGenerator to perform any cleanup (eg: releasing any held locks) as a result of acquiring new workflowId.
 */
public interface WorkflowIdGenerator {
    
    public long generateWorkflowId(String mitigationTemplate, TSDMetrics metrics);
    
    public void confirmAcquiringWorkflowId(String mitigationTemplate, long workflowId, TSDMetrics metrics);
}
