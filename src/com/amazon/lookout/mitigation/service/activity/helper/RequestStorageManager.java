package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.RequestType;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/** 
 * RequestStorageManager is responsible for managing storage of requests that come to the mitigationservice.
 *
 */
public interface RequestStorageManager {
    
    public long storeRequestForWorkflow(MitigationModificationRequest request, RequestType requestType, TSDMetrics metrics);
    
    public void updateRunIdForWorkflowRequest(String deviceName, long workflowId, String runId, RequestType requestType, TSDMetrics metrics);
}
