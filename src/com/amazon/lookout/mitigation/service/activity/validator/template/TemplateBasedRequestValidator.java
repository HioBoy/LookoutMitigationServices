package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.beans.ConstructorProperties;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.IPTablesJsonValidator;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.google.common.collect.ImmutableMap;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

/**
 * TemplateBasedRequestValidator is responsible for performing deep validations based on the template passed as input to the request.
 * Based on the template, this validator invokes the appropriate validator to delegate the validation check to.
 *
 */
@ThreadSafe
public class TemplateBasedRequestValidator {
    private static final Log LOG = LogFactory.getLog(TemplateBasedRequestValidator.class);
    
    private static final String MITIGATION_TEMPLATE_KEY = "MitigationTemplate";
    
    private final EdgeLocationsHelper edgeLocationsHelper;
    private final MetricsFactory metricsFactory;
    
    // Map of templateName -> ServiceTemplateValidator which is responsible for validating this template.
    private final ImmutableMap<String, ServiceTemplateValidator> serviceTemplateValidatorMap;
    
    /**
     * @param serviceSubnetsMatcher ServiceSubnetsMatcher is taken as an input in the constructor to allow for the service template specific validators to use
     *                              this matcher, in case they have to perform any subnet specific checks.
     */
    @ConstructorProperties({"serviceSubnetsMatcher", "edgeLocationsHelper", "metricsFactory"})
    public TemplateBasedRequestValidator(@Nonnull ServiceSubnetsMatcher serviceSubnetsMatcher, @Nonnull EdgeLocationsHelper edgeLocationsHelper,
            @Nonnull MetricsFactory metricsFactory) {
        Validate.notNull(serviceSubnetsMatcher);
        Validate.notNull(edgeLocationsHelper);
        Validate.notNull(metricsFactory);
        
        this.serviceTemplateValidatorMap = getServiceTemplateValidatorMap(serviceSubnetsMatcher);
        this.edgeLocationsHelper = edgeLocationsHelper;
        this.metricsFactory = metricsFactory;
    }
    
    /**
     * Validate request object passed to the MitigationAPI based on the template in the request. 
     * @param request Request passed to the MitigationAPI.
     * @param metrics
     * @return void. Returns nothing, but will propagate any exceptions thrown by the ServiceTemplateValidator whom this validation is delegated to.
     */
    public void validateRequestForTemplate(@Nonnull MitigationModificationRequest request, @Nonnull TSDMetrics metrics) {
        Validate.notNull(request);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("TemplateBasedRequestValidator.validateRequestForTemplate");
        try {
            String mitigationTemplate = request.getMitigationTemplate();
            subMetrics.addProperty(MITIGATION_TEMPLATE_KEY, mitigationTemplate);
            
            ServiceTemplateValidator templateBasedValidator = getValidator(mitigationTemplate);
            
            LOG.debug("Calling validator for service: " + templateBasedValidator.getServiceNameToValidate() + " for template: " + mitigationTemplate + " for request: " +
                      ReflectionToStringBuilder.toString(request));
            templateBasedValidator.validateRequestForTemplate(request, mitigationTemplate);
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
     * Returns an instance of IPTablesEdgeCustomerValidator.
     * @return ServiceTemplateValidator
     */
    private ServiceTemplateValidator getIPTablesEdgeCustomerValidator() {
        return new IPTablesEdgeCustomerValidator(new IPTablesJsonValidator());
    }
    
    private ServiceTemplateValidator getBlackWatchEdgeCustomerValidator() {
    	return new EdgeBlackWatchMitigationTemplateValidator(edgeLocationsHelper, metricsFactory);
    }

    /**
     * Returns map of templateName to ServiceTemplateValidator corresponding to the template.
     * @param serviceSubnetsMatcher ServiceSubnetsMatcher that is used by some of the ServiceTemplateValidators.
     * @return ImmutableMap of templateName to ServiceTemplateValidator corresponding to the template.
     */
    private ImmutableMap<String, ServiceTemplateValidator> getServiceTemplateValidatorMap(ServiceSubnetsMatcher serviceSubnetsMatcher) {
        ImmutableMap.Builder<String, ServiceTemplateValidator> serviceTemplateValidatorMapBuilder = ImmutableMap.builder();
        for (String mitigationTemplate : MitigationTemplate.values()) {
            switch (mitigationTemplate) {
            case MitigationTemplate.Router_RateLimit_Route53Customer:
                serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getRoute53SingleCustomerValidator(serviceSubnetsMatcher));
                break;
            case MitigationTemplate.Router_CountMode_Route53Customer:
                serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getRoute53SingleCustomerValidator(serviceSubnetsMatcher));
                break;
            case MitigationTemplate.IPTables_Mitigation_EdgeCustomer:
                serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getIPTablesEdgeCustomerValidator());
                break;
            case MitigationTemplate.BlackWatchPOP_EdgeCustomer:
            	serviceTemplateValidatorMapBuilder.put(mitigationTemplate, getBlackWatchEdgeCustomerValidator());
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
