package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;
import java.util.Map;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;

public interface HostStatusInfoHandler {

    public List<HostStatusInLocation> getHostsStatus(String location, TSDMetrics tsdMetrics);

}
