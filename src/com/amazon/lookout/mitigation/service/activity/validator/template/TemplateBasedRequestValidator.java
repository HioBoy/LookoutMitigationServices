package com.amazon.lookout.mitigation.service.activity.validator.template;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.google.common.collect.ImmutableMap;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;

/**
 * TemplateBasedRequestValidator is responsible for performing deep validations based on the template passed as input to the request.
 * Based on the template, this validator invokes the appropriate validator to delegate the validation check to.
 *
 */
@ThreadSafe
public class TemplateBasedRequestValidator {
    private static final Log LOG = LogFactory.getLog(TemplateBasedRequestValidator.class);
    
    private static final String MITIGATION_TEMPLATE_KEY = "MitigationTemplate";
    
    // Map of templateName -> ServiceTemplateValidator which is responsible for validating this template.
    private final ImmutableMap<String, ServiceTemplateValidator> serviceTemplateValidatorMap;
    
    /**
     * @param serviceSubnetsMatcher ServiceSubnetsMatcher is taken as an input in the constructor to allow for the service template specific validators to use
     *                              this matcher, in case they have to perform any subnet specific checks.
     */
    public TemplateBasedRequestValidator(@Nonnull ServiceSubnetsMatcher serviceSubnetsMatcher) {
        Validate.notNull(serviceSubnetsMatcher);
        serviceTemplateValidatorMap = getServiceTemplateValidatorMap(serviceSubnetsMatcher);
    }
    
    /**
     * Validate request object passed to the CreateMitigationAPI based on the template in the request. 
     * @param createRequest Request passed to the CreateMitigationAPI.
     * @param metrics
     * @return void. Returns nothing, but will propagate any exceptions thrown by the ServiceTemplateValidator whom this validation is delegated to.
     */
    public void validateCreateRequestForTemplate(@Nonnull MitigationModificationRequest createRequest, @Nonnull TSDMetrics metrics) {
        Validate.notNull(createRequest);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("TemplateBasedRequestValidator.validateCreateRequestForTemplate");
        try {
            String mitigationTemplate = createRequest.getMitigationTemplate();
            subMetrics.addProperty(MITIGATION_TEMPLATE_KEY, mitigationTemplate);
            
            ServiceTemplateValidator templateBasedValidator = getValidator(mitigationTemplate);
            
            LOG.debug("Calling validator for service: " + templateBasedValidator.getServiceNameToValidate() + " for template: " + mitigationTemplate + " for request: " +
                      ReflectionToStringBuilder.toString(createRequest));
            templateBasedValidator.validateRequestForTemplate(createRequest, mitigationTemplate);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Validate if the new mitigation can coexist with an existing mitigation. Different templates might dictate different rules for co-existence, hence delegating this
     * check to the ServiceTemplateValidator.
     * @param templateForNewDefinition Template used for the new mitigation.
     * @param nameForNewDefinition MitigationName of the new mitigation.
     * @param newDefinition MitigationDefinition of the new mitigation.
     * @param templateForExistingDefinition Template using which the existing mitigation was created. 
     * @param nameForExistingDefinition MitigationName of the existing mitigation.
     * @param existingDefinition MitigationDefinition of the existing mitigation.
     */
    public void validateCoexistenceForTemplateAndDevice(@Nonnull String templateForNewDefinition, @Nonnull String nameForNewDefinition, @Nonnull MitigationDefinition newDefinition, 
                                                        @Nonnull String templateForExistingDefinition, @Nonnull String nameForExistingDefinition, 
                                                        @Nonnull MitigationDefinition existingDefinition) {
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(nameForNewDefinition);
        Validate.notNull(newDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(nameForExistingDefinition);
        Validate.notNull(existingDefinition);
        
        ServiceTemplateValidator templateBasedValidator = getValidator(templateForNewDefinition);
        
        // Delegate the check to template specific validator.
        templateBasedValidator.validateCoexistenceForTemplateAndDevice(templateForNewDefinition, nameForNewDefinition, newDefinition, templateForExistingDefinition, 
                                                                       nameForExistingDefinition, existingDefinition);
        
        // Perform a symmetrical check if the templates are different, to ensure we check the validator for the existing mitigation as well.
        if (!templateForExistingDefinition.equals(templateForNewDefinition)) {
            templateBasedValidator = getValidator(templateForExistingDefinition);
            
            // Delegate the check to template specific validator.
            templateBasedValidator.validateCoexistenceForTemplateAndDevice(templateForExistingDefinition, nameForExistingDefinition, existingDefinition, 
                                                                           templateForNewDefinition, nameForNewDefinition, newDefinition);
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
     * Returns an instance of Route53SingleCustomerMitigationValidator. Protected to allow for unit-testing.
     * @param subnetsMatcher
     * @return Route53SingleCustomerMitigationValidator
     */
    protected Route53SingleCustomerMitigationValidator getRoute53SingleCustomerValidator(ServiceSubnetsMatcher subnetsMatcher) {
        return new Route53SingleCustomerMitigationValidator(subnetsMatcher);
    }

    /**
     * Returns map of templateName to ServiceTemplateValidator corresponding to the template.
     * @param serviceSubnetsMatcher ServiceSubnetsMatcher that is used by some of the ServiceTemplateValidators.
     * @return ImmutableMap of templateName to ServiceTemplateValidator corresponding to the template.
     */
    private ImmutableMap<String, ServiceTemplateValidator> getServiceTemplateValidatorMap(ServiceSubnetsMatcher serviceSubnetsMatcher) {
        ImmutableMap.Builder<String, ServiceTemplateValidator> serviceTemplateValidatorMapBuilder = new ImmutableMap.Builder<>();
        for (String mitigationTemplate : MitigationTemplate.values()) {
            switch (mitigationTemplate) {
            case MitigationTemplate.Router_RateLimit_Route53Customer:
                serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getRoute53SingleCustomerValidator(serviceSubnetsMatcher));
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
