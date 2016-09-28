package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.List;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;

public interface BlackWatchMitigationInfoHandler {
    public void deactivateMitigation(String mitigationId, MitigationActionMetadata actionMetadata);
    public void changeOwnerARN(String mitigationId, String newOwnerARN, String expectedOwnerARN, MitigationActionMetadata actionMetadata);
    public List<BlackWatchMitigationDefinition> getBlackWatchMitigations(
            String mitigationId, String resourceId, String resourceType,
            String ownerARN, long maxNumberOfEntriesToReturn,
            TSDMetrics tsdMetrics);
}
