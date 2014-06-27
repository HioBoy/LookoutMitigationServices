package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.activities.model.MitigationMetadata;

/** 
 * RequestInfoHandler is responsible for retrieving request info from a data store.
 *
 */
public interface RequestInfoHandler {
    
    public MitigationNameAndRequestStatus getMitigationNameAndRequestStatus(String deviceName, long jobId, TSDMetrics metrics);
    
    public MitigationMetadata getMitigationMetadata(String deviceName, long jobId, TSDMetrics tsdMetrics);
    
}
