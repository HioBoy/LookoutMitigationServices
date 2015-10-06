package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.Set;

import lombok.NonNull;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * Locations helper for route53 single customer templates.
 */
public class Route53SingleCustomerTemplateLocationsHelper implements TemplateBasedLocationsHelper {
    private static final Log LOG = LogFactory.getLog(Route53SingleCustomerTemplateLocationsHelper.class);
    
    private final EdgeLocationsHelper locationsHelper;
    private final Set<String> popsWithCiscoRouter;
    
    @ConstructorProperties({"edgeLocationsHelper", "popsWithCiscoRouter"})
    public Route53SingleCustomerTemplateLocationsHelper(@NonNull EdgeLocationsHelper locationsHelper, @NonNull Set<String> popsWithCiscoRouter) {
        this.locationsHelper = locationsHelper;
        this.popsWithCiscoRouter = popsWithCiscoRouter; // Could be empty for beta.
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
        
        if (locationsToDeploy.isEmpty()) {
            String msg = "Got no locations to deploy to for mitigation: " + request.getMitigationName() + " for service: " + request.getServiceName();
            LOG.info(msg + " request: " + ReflectionToStringBuilder.toString(request));
            throw new InternalServerError500(msg);
        }
        
        return locationsToDeploy;
    }

}
