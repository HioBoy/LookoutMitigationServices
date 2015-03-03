package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * ActiveMitigationInfoHandler is responsible for retrieving data from the ACTIVE_MITIGATIONS table.
 *
 */
public interface ActiveMitigationInfoHandler {
    
    public List<ActiveMitigationDetails> getActiveMitigationsForService(String serviceName, String deviceName, List<String> locations, TSDMetrics tsdMetrics);
    
    public UpdateItemResult markMitigationAsDefunct(String serviceName, String mitigationName, String deviceName, String location, long jobId, long lastDeployDate, TSDMetrics tsdMetrics);
}
