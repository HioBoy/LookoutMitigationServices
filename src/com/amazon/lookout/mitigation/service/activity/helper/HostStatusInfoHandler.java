package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;

public interface HostStatusInfoHandler {

    public List<HostStatusInLocation> getHostsStatus(LocationState locationState, TSDMetrics tsdMetrics);

}
