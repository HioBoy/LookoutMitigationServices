package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;

public interface WorkflowIdGenerator {
	
	public long generateWorkflowId(String mitigationTemplate, TSDMetrics metrics);
	
	public void confirmAcquiringWorkflowId(String mitigationTemplate, long workflowId, TSDMetrics metrics);
}
