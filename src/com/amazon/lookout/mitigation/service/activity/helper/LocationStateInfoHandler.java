package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;
import java.util.Map;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.constants.DeviceName;

public interface LocationStateInfoHandler {
    public LocationState getLocationState(String location, TSDMetrics tsdMetrics);
    public BlackWatchLocation convertLocationState(LocationState in);
    public List<BlackWatchLocation> getAllBlackWatchLocations(TSDMetrics tsdMetrics);
    public void updateBlackWatchLocationAdminIn(String location, boolean adminIn, String reason, String locationType,
                                                String operationId, String changeId, boolean forced, TSDMetrics tsdMetrics);
    public LocationState requestHostStatusChange(String location, String hostName, HostStatusEnum requestedStatus,
                                                 String changeReason, String changeUser, String changeHost,
                                                 List<String> relatedLinks, boolean createMissingHost,
                                                 TSDMetrics tsdMetrics);
    public List<String> getPendingOperationLocks(String location, TSDMetrics tsdMetrics);
    public boolean validateOtherStacksInService(String location, TSDMetrics tsdMetrics);
    public boolean checkIfLocationIsOperational(String location, TSDMetrics tsdMetrics);
}
