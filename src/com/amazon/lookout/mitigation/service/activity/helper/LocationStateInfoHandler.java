package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;

public interface LocationStateInfoHandler {
    public List<BlackWatchLocation> getBlackWatchLocation(String region, TSDMetrics tsdMetrics);
    public void updateBlackWatchLocationAdminIn(String location, boolean adminIn, String reason, String locationType, TSDMetrics tsdMetrics);
}
