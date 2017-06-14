package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;

/**
 * ActiveMitigationInfoHandler is responsible for retrieving data from the ACTIVE_MITIGATIONS table.
 *
 */
public interface ActiveMitigationInfoHandler {
    
    public List<ActiveMitigationDetails> getActiveMitigationsForService(String serviceName, String deviceName, List<String> locations, TSDMetrics tsdMetrics);
}

