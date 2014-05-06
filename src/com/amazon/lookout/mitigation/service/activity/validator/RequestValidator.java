package com.amazon.lookout.mitigation.service.activity.validator;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.lookout.mitigation.service.DeleteMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Sets;

/**
 * RequestValidator is a basic validator for requests to ensure the requests are well-formed and contain all the required inputs.
 * This validator performs template-agnostic validation - it doesn't dive deep into specific requirements of each template, the TemplateBasedRequestValidator
 * is responsible for such checks.  
 *
 */
@ThreadSafe
public class RequestValidator {
    private static final Log LOG = LogFactory.getLog(RequestValidator.class);
    
    // Maintain a Set of the mitigationTemplate, making it easier to search if a single template is part of the set of defined templates.
    private final Set<String> mitigationTemplates = Sets.newHashSet(MitigationTemplate.values());
    
    // Maintain a Set of the supported serviceNames, making it easier to search if a single serviceName is part of the set of defined serviceNames.
    private final Set<String> serviceNames = Sets.newHashSet(ServiceName.values());
    
    /**
     * Validates if the request object passed to the CreateMitigationAPI is valid.
     * @param request MitigationModificationRequest representing the input to the CreateMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateCreateRequest(@Nonnull MitigationModificationRequest request) {
        Validate.notNull(request);
        
        validateCommonRequestParameters(request.getMitigationName(), request.getMitigationTemplate(), request.getServiceName(), request.getMitigationActionMetadata());
        
        if (request.getMitigationDefinition() == null) {
            String msg = "No mitigation definition found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We only check for the constraint and not the action, since for some templates, it is valid for requests to not have any actions.
        // Template-based validations should take care of specific checks related to their template.
        if (request.getMitigationDefinition().getConstraint() == null) {
            String msg = "No constraint found for mitigation definition.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the DeleteMitigationAPI is valid.
     * @param request Instance of DeleteMitigationFromAllLocationsRequest representing the input to the DeleteMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateDeleteRequest(@Nonnull DeleteMitigationRequest request) {
        Validate.notNull(request);
        validateCommonRequestParameters(request.getMitigationName(), request.getMitigationTemplate(), request.getServiceName(), request.getMitigationActionMetadata());
        
        if ((request.getLocation() != null) || !request.getLocation().isEmpty()) {
            String msg = "Locations not expected to be set for delete request since we delete the mitigation from all locations, instead found: " + request.getLocation();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (request.getMitigationDefinition() != null) {
            String msg = "Mitigation Definition not expected to be set for delete request, instead found: " + request.getMitigationDefinition();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (request.getMitigationVersion() < 1) {
            String msg = "Version of the mitigation to be deleted should be set to >=1, instead found: " + request.getMitigationVersion();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Private helper method to validate the common parameters for some of the modification requests.
     * @param mitigationName Name of the mitigation passed in the request by the client.
     * @param mitigationTemplate Template corresponding to the mitigation in the request by the client.
     * @param serviceName Service corresponding to the mitigation in the request by the client.
     * @param actionMetadata Instance of MitigationActionMetadata specifying some of the metadata related to this action.
     */
    private void validateCommonRequestParameters(String mitigationName, String mitigationTemplate, String serviceName, MitigationActionMetadata actionMetadata) {
        if (StringUtils.isEmpty(mitigationName)) {
            String msg = "Null or empty mitigation name found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(mitigationTemplate)) {
            String msg = "Null or empty mitigation template found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!mitigationTemplates.contains(mitigationTemplate)) {
            String msg = "Invalid mitigation template found. Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(serviceName)) {
            String msg = "Null or empty service name found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!serviceNames.contains(serviceName)) {
            String msg = "Invalid service name found. Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (actionMetadata == null) {
            String msg = "No MitigationActionMetadata found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if ((actionMetadata.getUser() == null) || (actionMetadata.getUser().isEmpty())) {
            String msg = "No user defined in the mitigation action metadata.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getToolName())) {
            String msg = "No tool specified in the mitigation action metadata.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getDescription())) {
            String msg = "No description specified in the mitigation action metadata.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
}
