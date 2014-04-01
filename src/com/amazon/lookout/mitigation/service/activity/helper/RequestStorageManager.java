package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.RequestType;

/** 
 * RequestStorageManager is responsible for managing storage of requests that come to the mitigationservice.
 *
 */
public interface RequestStorageManager {
    
    public long storeRequestForWorkflow(MitigationModificationRequest request, RequestType requestType, TSDMetrics metrics);
}
