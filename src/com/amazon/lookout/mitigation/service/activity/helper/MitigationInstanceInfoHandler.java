package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;

public interface MitigationInstanceInfoHandler {

    public List<MitigationInstanceStatus> getMitigationInstanceStatus(String deviceName, long jobId, TSDMetrics tsdMetrics);

    public List<LocationDeploymentInfo> getLocationDeploymentInfoOnLocation(String deviceName, String location,
             int maxNumberOfHistoryEntriesToFetch, Long exclusiveLastEvaluatedTimestamp, TSDMetrics tsdMetrics);
}