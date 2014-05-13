package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * Locations helper for route53 single customer templates.
 */
public class Route53SingleCustomerTemplateLocationsHelper implements TemplateBasedLocationsHelper {
    
    private final EdgeLocationsHelper locationsHelper;
    private final Set<String> popsWithJuniperRouters;
    
    public Route53SingleCustomerTemplateLocationsHelper(@Nonnull EdgeLocationsHelper locationsHelper, @Nonnull Set<String> popsWithJuniperRouters) {
        Validate.notNull(locationsHelper);
        this.locationsHelper = locationsHelper;
        
        Validate.notNull(popsWithJuniperRouters); // Could be empty for beta.
        this.popsWithJuniperRouters = popsWithJuniperRouters;
    }
    
    /**
     * For templates that use this locations helper, locations for deployment are all non-blackwatch POPs which have Juniper routers.
     * @param request MitigationModificationRequest instance sent by the client.
     * @return Set<String> representing a set of POPs where BW doesn't have any hosts and the POP has Juniper routers.
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request) {
        Set<String> locationsToDeploy = new HashSet<String>();
        for (String location : locationsHelper.getAllNonBlackwatchPOPs() ) {
            if (popsWithJuniperRouters.contains(location.toLowerCase())) {
                locationsToDeploy.add(location);
            }
        }
        return locationsToDeploy;
    }

}
