package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
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
    private final Set<String> popsWithJuniperRouter;
    
    @ConstructorProperties({"edgeLocationsHelper", "popsWithJuniperRouter"})
    public Route53SingleCustomerTemplateLocationsHelper(@Nonnull EdgeLocationsHelper locationsHelper, @Nonnull Set<String> popsWithJuniperRouter) {
        Validate.notNull(locationsHelper);
        this.locationsHelper = locationsHelper;
        
        Validate.notNull(popsWithJuniperRouter); // Could be empty for beta.
        this.popsWithJuniperRouter = popsWithJuniperRouter;
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
            if (popsWithJuniperRouter.contains(location.toUpperCase())) {
                locationsToDeploy.add(location);
            }
        }
        return locationsToDeploy;
    }

}
