package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;

/** 
 * RequestInfoHandler is responsible for retrieving request info from a data store.
 *
 */
public interface RequestInfoHandler {
    
    public MitigationNameAndRequestStatus getMitigationNameAndRequestStatus(String deviceName, String templateName, long jobId, TSDMetrics metrics);
    
    public MitigationRequestDescription getMitigationRequestDescription(String deviceName, long jobId, TSDMetrics tsdMetrics);
    
    public List<MitigationRequestDescription> getMitigationRequestDescriptionsForMitigation(String serviceName, String deviceName,
            String mitigationName, TSDMetrics tsdMetrics);
    
    public List<MitigationRequestDescriptionWithLocations> getMitigationHistoryForMitigation(
            String serviceName, String deviceName, String mitigationName, Integer exclusiveStartVersion, 
            Integer maxNumberOfHistoryEntriesToFetch, TSDMetrics tsdMetrics);
    
    public List<MitigationRequestDescriptionWithLocations> getOngoingRequestsDescription(String serviceName, String deviceName, List<String> locations, TSDMetrics tsdMetrics);

    public MitigationRequestDescriptionWithLocations getMitigationDefinition(String deviceName, String serviceName, String mitigationName, int mitigationVersion,
            TSDMetrics tsdMetrics);
}
