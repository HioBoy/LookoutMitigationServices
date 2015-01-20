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
    private final Set<String> popsWithCiscoRouter;
    
    @ConstructorProperties({"edgeLocationsHelper", "popsWithCiscoRouter"})
    public Route53SingleCustomerTemplateLocationsHelper(@Nonnull EdgeLocationsHelper locationsHelper, @Nonnull Set<String> popsWithCiscoRouter) {
        Validate.notNull(locationsHelper);
        this.locationsHelper = locationsHelper;
        
        Validate.notNull(popsWithCiscoRouter); // Could be empty for beta.
        this.popsWithCiscoRouter = popsWithCiscoRouter;
    }
    
    /**
     * For templates that use this locations helper, locations for deployment are all non-blackwatch POPs which have a Juniper router (don't have Cisco router)
     * @param request MitigationModificationRequest instance sent by the client.
     * @return Set<String> representing a set of POPs where BW doesn't have any hosts and the POP has Juniper router.
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request) {
        Set<String> locationsToDeploy = new HashSet<String>();
        for (String location : locationsHelper.getAllNonBlackwatchClassicPOPs() ) {
            if (!popsWithCiscoRouter.contains(location.toUpperCase())) {
                locationsToDeploy.add(location);
            }
        }
        return locationsToDeploy;
    }

}
