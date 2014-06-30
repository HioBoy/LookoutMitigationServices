package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.Set;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.model.RequestType;

/** 
 * RequestStorageManager is responsible for managing storage of requests that come to the mitigationservice.
 *
 */
public interface RequestStorageManager {
    
    public long storeRequestForWorkflow(MitigationModificationRequest request, Set<String> locations, RequestType requestType, TSDMetrics metrics);
    
    public void updateRunIdForWorkflowRequest(String deviceName, long workflowId, String runId, RequestType requestType, TSDMetrics metrics);
}
