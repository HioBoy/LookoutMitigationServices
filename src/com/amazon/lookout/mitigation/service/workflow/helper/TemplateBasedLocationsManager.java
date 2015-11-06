package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.NonNull;

import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
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
    
    @ConstructorProperties({"route53SingleCustomerHelper", "blackWatchTemplateLocationHelper"})
    public TemplateBasedLocationsManager(@NonNull Route53SingleCustomerTemplateLocationsHelper route53SingleCustomerHelper,
            BlackWatchTemplateLocationHelper blackWatchTemplateLocationHelper) {
        ImmutableMap.Builder<String, TemplateBasedLocationsHelper> mapBuilder = new ImmutableMap.Builder<>();
        mapBuilder.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53SingleCustomerHelper);
        // Router_CountMode_Route53Customer is currently used only by external monitors, and
        // it shares the locationHelper with the Router_RateLimit_Route53Customer template
        mapBuilder.put(MitigationTemplate.Router_CountMode_Route53Customer, route53SingleCustomerHelper);
        mapBuilder.put(MitigationTemplate.IPTables_Mitigation_EdgeCustomer,
                new IPTablesEdgeCustomerTemplateLocationsHelper());
        mapBuilder.put(MitigationTemplate.Blackhole_Mitigation_ArborCustomer,
                new ArborBlackholeLocationsHelper());
        mapBuilder.put(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, blackWatchTemplateLocationHelper);
        mapBuilder.put(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer, blackWatchTemplateLocationHelper);
        
        templateBasedLocationsHelpers = mapBuilder.build();
    }
    
    /**
     * Determine the locations where the workflow should run for the request passed as input.
     * @param request MitigationModificationRequest representing the request passed by the client.
     * @return Set<String> representing the locations where this workflow must run, based on the decision made by the template specific locations helper,
     *                     if there is one configured for the template in the request, else we simply fallback to the locations specified in the request object.
     */
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request, TSDMetrics tsdMetrics) {
        String templateName = request.getMitigationTemplate();
        if (templateBasedLocationsHelpers.containsKey(templateName)) {
            TemplateBasedLocationsHelper templateBasedLocationsHelper = templateBasedLocationsHelpers.get(templateName);
            return new HashSet<String>(templateBasedLocationsHelper.getLocationsForDeployment(request, tsdMetrics));
        }
        
        Set<String> locationsInRequest = getLocationsFromRequest(request);
        if (locationsInRequest != null) {
            return locationsInRequest;
        }
        
        throw new IllegalArgumentException("Can not find locations to deploy for request " + request);
    }
    
    
    /**
     * Get locations from MitigationModificationRequest request.
     * if request type is create or edit, we will return the locations inside the request.
     * If locations is empty for create or edit request, IllegalArgumentException is thrown.
     * For other type of request, return null.
     * @param request : MitigationModificationRequest
     * @return null or set of locations
     */
    public static Set<String> getLocationsFromRequest(MitigationModificationRequest request) {
        Validate.notNull(request);
        
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
        } else {
            return null;
        }
    }
}
