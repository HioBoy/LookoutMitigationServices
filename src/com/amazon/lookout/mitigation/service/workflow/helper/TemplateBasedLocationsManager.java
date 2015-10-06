package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.NonNull;
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
    
    public TemplateBasedLocationsManager(@NonNull Route53SingleCustomerTemplateLocationsHelper route53SingleCustomerHelper) {
        ImmutableMap.Builder<String, TemplateBasedLocationsHelper> mapBuilder = new ImmutableMap.Builder<>();
        mapBuilder.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53SingleCustomerHelper);
        // Router_CountMode_Route53Customer is currently used only by external monitors, and
        // it shares the locationHelper with the Router_RateLimit_Route53Customer template
        mapBuilder.put(MitigationTemplate.Router_CountMode_Route53Customer, route53SingleCustomerHelper);
        mapBuilder.put(MitigationTemplate.IPTables_Mitigation_EdgeCustomer,
                new IPTablesEdgeCustomerTemplateLocationsHelper());
        mapBuilder.put(MitigationTemplate.Blackhole_Mitigation_ArborCustomer,
                new ArborBlackholeLocationsHelper());
        
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
        
        List<String> requestLocations = null;
        if (request instanceof CreateMitigationRequest) {
            requestLocations = ((CreateMitigationRequest) request).getLocations();
            Validate.notEmpty(requestLocations, "locations may not be empty for create");
        } else if (request instanceof EditMitigationRequest) {
            requestLocations = ((EditMitigationRequest) request).getLocation();
            Validate.notEmpty(requestLocations, "location may not be empty for edit");
        }
        
        if (requestLocations != null) {
            return new HashSet<>(requestLocations);
        }
        
        return new HashSet<>();
    }
}
