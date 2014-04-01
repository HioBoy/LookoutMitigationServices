package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * RequestStorageHandler is responsible for persisting a request into a data store.
 * For requests that require a new workflow to be created, as part of the storage process, it also identifies the workflowId to be used.
 *
 */
public interface RequestStorageHandler {
    
    public long storeRequestForWorkflow(MitigationModificationRequest mitigationRequest, TSDMetrics metrics);
}
