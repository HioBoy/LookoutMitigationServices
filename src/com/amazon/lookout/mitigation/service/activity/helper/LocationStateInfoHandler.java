package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;

public interface LocationStateInfoHandler {
    public LocationState getLocationState(String location, TSDMetrics tsdMetrics);
    public BlackWatchLocation convertLocationState(LocationState in);
    public List<BlackWatchLocation> getAllBlackWatchLocations(TSDMetrics tsdMetrics);
    public void updateBlackWatchLocationAdminIn(String location, boolean adminIn, String reason, String locationType,
                                                TSDMetrics tsdMetrics);
    public LocationState requestHostStatusChange(String location, String hostName, HostStatusEnum requestedStatus,
                                                 String changeReason, String changeUser, String changeHost,
                                                 List<String> relatedLinks, boolean createMissingHost,
                                                 TSDMetrics tsdMetrics);
}
