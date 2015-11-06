package com.amazon.lookout.mitigation.service.workflow.helper;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Locations helper for Blackhole_Mitigation_ArborCustomer template.
 */
public class ArborBlackholeLocationsHelper implements TemplateBasedLocationsHelper {
    /**
     * For Blackhole_Mitigation_ArborCustomer template default location is Worldwide.
     * @param request MitigationModificationRequest instance sent by the client.
     * @return Set<String> with Worldwide location.
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request, TSDMetrics tsdMetrics) {
        return ImmutableSet.of(StandardLocations.ARBOR);
    }
}
