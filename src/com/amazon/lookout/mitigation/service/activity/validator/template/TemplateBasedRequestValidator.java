package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.beans.ConstructorProperties;

import javax.annotation.concurrent.ThreadSafe;

import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.IPTablesJsonValidator;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.google.common.collect.ImmutableMap;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazonaws.services.s3.AmazonS3;

/**
 * TemplateBasedRequestValidator is responsible for performing deep validations based on the template passed as input to the request.
 * Based on the template, this validator invokes the appropriate validator to delegate the validation check to.
 *
 */
@ThreadSafe
public class TemplateBasedRequestValidator {
    private static final Log LOG = LogFactory.getLog(TemplateBasedRequestValidator.class);
    
    private static final String MITIGATION_TEMPLATE_KEY = "MitigationTemplate";
    
    private final AmazonS3 blackWatchS3Client;

    // Map of templateName -> ServiceTemplateValidator which is responsible for validating this template.
    private final ImmutableMap<String, ServiceTemplateValidator> serviceTemplateValidatorMap;
    
    @ConstructorProperties({"blackWatchS3Client"})
    public TemplateBasedRequestValidator(@NonNull AmazonS3 blackWatchS3Client) 
    {
        
        this.blackWatchS3Client = blackWatchS3Client;
        
        // this line should be the last line of constructor, as it might relies on the variable assigned before.
        this.serviceTemplateValidatorMap = getServiceTemplateValidatorMap();
    }
    
    /**
     * Validate request object passed to the MitigationAPI based on the template in the request. 
     * @param request Request passed to the MitigationAPI.
     * @param metrics
     * @return void. Returns nothing, but will propagate any exceptions thrown by the ServiceTemplateValidator whom this validation is delegated to.
     */
    public void validateRequestForTemplate(@NonNull MitigationModificationRequest request, @NonNull TSDMetrics metrics) {
        
        TSDMetrics subMetrics = metrics.newSubMetrics("TemplateBasedRequestValidator.validateRequestForTemplate");
        try {
            String mitigationTemplate = request.getMitigationTemplate();
            subMetrics.addProperty(MITIGATION_TEMPLATE_KEY, mitigationTemplate);
            
            ServiceTemplateValidator templateBasedValidator = getValidator(mitigationTemplate);
            
            LOG.debug("Calling validator for service: for template: " + mitigationTemplate + " for request: " +
                      ReflectionToStringBuilder.toString(request));
            templateBasedValidator.validateRequestForTemplate(request, mitigationTemplate, metrics);
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Return the ServiceTemplateValidator based on the mitigationTemplate passed as input. Protected for unit-testing.
     * @param mitigationTemplate MitigationTemplate used in the request.
     * @return ServiceTemplateValidator corresponding to the mitigation template passed as input. 
     *         If a validator cannot be found for this template, we throw an InternalServerError since this request should have been 
     *         validated to have a valid template when this check is performed.
     */
    protected ServiceTemplateValidator getValidator(String mitigationTemplate) {
        ServiceTemplateValidator templateBasedValidator = serviceTemplateValidatorMap.get(mitigationTemplate);
        if (templateBasedValidator == null) {
            String msg = "No check configured for mitigationTemplate: " + mitigationTemplate + ". There should be a validator associated with each valid mitigation template. ";
            LOG.error(msg);
            throw new InternalServerError500(msg);
        }
        return templateBasedValidator;
    }
    
    /**
     * Returns an instance of IPTablesEdgeCustomerValidator.
     * @return ServiceTemplateValidator
     */
    private ServiceTemplateValidator getIPTablesEdgeCustomerValidator() {
        return new IPTablesEdgeCustomerValidator(new IPTablesJsonValidator());
    }
    
    private ServiceTemplateValidator getBlackWatchEdgeCustomerValidator() {
        return new EdgeBlackWatchMitigationTemplateValidator(blackWatchS3Client);
    }

    private ServiceTemplateValidator getBlackWatchBorderValidator() {
        return new BlackWatchPerTargetBorderLocationTemplateValidator(blackWatchS3Client);
    }

    private ServiceTemplateValidator getBlackWatchEdgeValidator() {
        return new BlackWatchPerTargetEdgeLocationTemplateValidator(blackWatchS3Client);
    }

    private ServiceTemplateValidator getVantaValidator() {
        return new VantaLocationTemplateValidator(blackWatchS3Client);
    }
    
    /**
     * Returns map of templateName to ServiceTemplateValidator corresponding to the template.
     * @return ImmutableMap of templateName to ServiceTemplateValidator corresponding to the template.
     */
    private ImmutableMap<String, ServiceTemplateValidator> getServiceTemplateValidatorMap() {
        ImmutableMap.Builder<String, ServiceTemplateValidator> serviceTemplateValidatorMapBuilder = ImmutableMap.builder();
        for (String mitigationTemplate : MitigationTemplate.values()) {
            switch (mitigationTemplate) {
            case MitigationTemplate.IPTables_Mitigation_EdgeCustomer:
                serviceTemplateValidatorMapBuilder = serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getIPTablesEdgeCustomerValidator());
                break;
            case MitigationTemplate.BlackWatchPOP_EdgeCustomer:
                serviceTemplateValidatorMapBuilder = serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getBlackWatchEdgeCustomerValidator());
                break;
            case MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer:
                serviceTemplateValidatorMapBuilder = serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getBlackWatchBorderValidator());
                break;
            case MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer:
                serviceTemplateValidatorMapBuilder = serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getBlackWatchEdgeValidator());
                break;
            case MitigationTemplate.Vanta:
                serviceTemplateValidatorMapBuilder = serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getVantaValidator());
                break;
            default:
                String msg = "No check configured for mitigationTemplate: " + mitigationTemplate + ". Each template must be associated with some validation checks.";
                LOG.error(msg);
                // We throw an internal server error since at this point this request should have been checked for having valid inputs, including the template name. 
                throw new IllegalStateException(msg);
            }
        }
        return serviceTemplateValidatorMapBuilder.build();
    }
}
