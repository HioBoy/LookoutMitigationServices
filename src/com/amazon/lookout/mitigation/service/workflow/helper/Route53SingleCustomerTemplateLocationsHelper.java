package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * Locations helper for route53 single customer templates.
 */
public class Route53SingleCustomerTemplateLocationsHelper implements TemplateBasedLocationsHelper {
    
    private final EdgeLocationsHelper locationsHelper;
    
    public Route53SingleCustomerTemplateLocationsHelper(@Nonnull EdgeLocationsHelper locationsHelper) {
        Validate.notNull(locationsHelper);
        this.locationsHelper = locationsHelper;
    }
    
    /**
     * For templates that use this locations helper, locations for deployment are all non-blackwatch POPs.
     * @param request MitigationModificationRequest instance sent by the client.
     * @return Set<String> representing a set of POPs where BW doesn't have any hosts.
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request) {
        return locationsHelper.getAllNonBlackwatchPOPs();
    }

}
