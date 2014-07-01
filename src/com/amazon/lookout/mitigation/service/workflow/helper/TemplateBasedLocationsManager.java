package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.google.common.collect.ImmutableMap;

/**
 * LocationsHelper to determine locations for deployment based on the request made by the client and the template used for the request.
 * It delegates the decision of the locations to the appropriate helper based on the template.
 *
 */
public class TemplateBasedLocationsManager {
    // Maintain a map of templateName to the locationsHelper for the template.
    private final ImmutableMap<String, TemplateBasedLocationsHelper> templateBasedLocationsHelpers;
    
    public TemplateBasedLocationsManager(@Nonnull Route53SingleCustomerTemplateLocationsHelper route53SingleCustomerHelper) {
        Validate.notNull(route53SingleCustomerHelper);
        
        ImmutableMap.Builder<String, TemplateBasedLocationsHelper> mapBuilder = new ImmutableMap.Builder<>();
        mapBuilder.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53SingleCustomerHelper);
        
        templateBasedLocationsHelpers = mapBuilder.build();
    }
    
    /**
     * Determine the locations where the workflow should run for the request passed as input.
     * @param request MitigationModificationRequest representing the request passed by the client.
     * @return Set<String> representing the locations where this workflow must run, based on the decision made by the template specific locations helper,
     *                     if there is one configured for the template in the request, else we simply fallback to the locations specified in the request object.
     */
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request) {
        String templateName = request.getMitigationTemplate();
        if (templateBasedLocationsHelpers.containsKey(templateName)) {
            TemplateBasedLocationsHelper templateBasedLocationsHelper = templateBasedLocationsHelpers.get(templateName);
            return new HashSet<String>(templateBasedLocationsHelper.getLocationsForDeployment(request));
        }
        
        if (request instanceof CreateMitigationRequest) {
            return new HashSet<String>(((CreateMitigationRequest) request).getLocations());
        }
        
        if (request instanceof EditMitigationRequest) {
            return new HashSet<String>(((EditMitigationRequest) request).getLocation());
        }
        return new HashSet<String>();
    }
}
