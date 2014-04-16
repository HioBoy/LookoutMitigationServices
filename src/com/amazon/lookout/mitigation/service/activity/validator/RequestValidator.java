package com.amazon.lookout.mitigation.service.activity.validator;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        
        if (StringUtils.isEmpty(request.getMitigationName())) {
            String msg = "Null or empty mitigation name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(request.getMitigationTemplate())) {
            String msg = "Null or empty mitigation template found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!mitigationTemplates.contains(request.getMitigationTemplate())) {
            String msg = "Invalid mitigation template found in request: " + ReflectionToStringBuilder.toString(request) + 
                         ". Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(request.getServiceName())) {
            String msg = "Null or empty service name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!serviceNames.contains(request.getServiceName())) {
            String msg = "Invalid service name found in request: " + ReflectionToStringBuilder.toString(request) + 
                         ". Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (request.getMitigationDefinition() == null) {
            String msg = "No mitigation definition found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We only check for the constraint and not the action, since for some templates, it is valid for requests to not have any actions.
        // Template-based validations should take care of specific checks related to their template.
        if (request.getMitigationDefinition().getConstraint() == null) {
            String msg = "No constraint found for mitigation definition in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (request.getMitigationActionMetadata() == null) {
            String msg = "No MitigationActionMetadata found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        MitigationActionMetadata actionMetadata = request.getMitigationActionMetadata();
        if ((actionMetadata.getUser() == null) || (actionMetadata.getUser().isEmpty())) {
            String msg = "No user defined in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getToolName())) {
            String msg = "No tool specified in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getDescription())) {
            String msg = "No description specified in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

}
