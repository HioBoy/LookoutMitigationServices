package com.amazon.lookout.mitigation.service.workflow.helper;

import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;

import java.util.Set;

/**
 * Locations helper for IPTables_Mitigation_EdgeCustomer template.
 */
public class IPTablesEdgeCustomerTemplateLocationsHelper implements TemplateBasedLocationsHelper {
    /**
     * For IPTables_Mitigation_EdgeCustomer template default location is EdgeWorldwide.
     * @param request MitigationModificationRequest instance sent by the client.
     * @return Set<String> with EdgeWorldwide location.
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request) {
        return Sets.newHashSet(StandardLocations.EDGE_WORLD_WIDE);
    }
}
