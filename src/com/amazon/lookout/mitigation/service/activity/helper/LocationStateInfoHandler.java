package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchLocations;

public interface LocationStateInfoHandler {
    public List<BlackWatchLocations> getBlackWatchLocations(TSDMetrics tsdMetrics);
}
