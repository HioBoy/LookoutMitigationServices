package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.net.IPUtils;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

/**
 * Validator for Route53's single customer based mitigation templates.
 */
@ThreadSafe
public class Route53SingleCustomerMitigationValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(Route53SingleCustomerMitigationValidator.class);
    
    // A list of chars that cannot be in the filterName as they may affect the configuration file due to the filterName being stored in a comment.
    // Refer original router mitigation UI code: http://tiny/11bh4sghs/codeamazpackLookblob58b6src
    private static final String INVALID_ROUTER_MITIGATION_NAME_CHARS = "\n|\r|\u0085|\u2028|\u2029|/\\*|\\*/";
    private static final Pattern INVALID_ROUTER_MITIGATION_NAME_PATTERN = Pattern.compile(INVALID_ROUTER_MITIGATION_NAME_CHARS);
        
    private final ServiceSubnetsMatcher serviceSubnetsMatcher;
    
    public Route53SingleCustomerMitigationValidator(@Nonnull ServiceSubnetsMatcher serviceSubnetsMatcher) {
        Validate.notNull(serviceSubnetsMatcher);
        this.serviceSubnetsMatcher = serviceSubnetsMatcher;
    }
    
    @Override
    public void validateRequestForTemplate(@Nonnull MitigationModificationRequest request, @Nonnull String mitigationTemplate) {
        Validate.notNull(request);
        Validate.notEmpty(mitigationTemplate);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
        if (deviceNameAndScope == null) {
            String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY + ": No DeviceNameAndScope mapping found for template: " + 
                         mitigationTemplate + ". Request being validated: " + ReflectionToStringBuilder.toString(request);
            LOG.warn(msg);
            throw new InternalServerError500(msg);
        }
        
        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope);
    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Route53;
    }

    @Override
    public void validateRequestForTemplateAndDevice(@Nonnull MitigationModificationRequest request, @Nonnull String mitigationTemplate, 
                                                    @Nonnull DeviceNameAndScope deviceNameAndScope) {
        Validate.notNull(request);
        Validate.notEmpty(mitigationTemplate);
        Validate.notNull(deviceNameAndScope);
        
        String mitigationName = request.getMitigationName();
        MitigationDefinition mitigationDefinition = request.getMitigationDefinition();
        Constraint mitigationConstraint = mitigationDefinition.getConstraint();
        ActionType mitigationAction = mitigationDefinition.getAction();
        List<String> locationsToApplyMitigation = request.getLocation();
        
        validateMitigationName(mitigationName, mitigationTemplate);
        validateLocationsToApply(locationsToApplyMitigation, mitigationTemplate);
        validateActionType(mitigationAction, mitigationTemplate);
        validateMitigationConstraint(mitigationConstraint, mitigationTemplate);
    }
    
    private void validateMitigationName(String mitigationName, String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            Matcher matcher = INVALID_ROUTER_MITIGATION_NAME_PATTERN.matcher(mitigationName);
            if (matcher.find()) {
                String msg = "Invalid mitigationName + " + mitigationName + " found for template: " + mitigationTemplate + ". Name cannot contain any control characters.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            break;
        }
    }
    
    private void validateLocationsToApply(List<String> locationsToApplyMitigation, String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            if ((locationsToApplyMitigation != null) && !locationsToApplyMitigation.isEmpty()) {
                String msg = "For the template: " + mitigationTemplate + " no locations are expected to be provided in the request, since this mitigation is " +
                             "expected to be applied to all non-Blackwatch POPs.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            break;
        }
    }
    
    private void validateActionType(ActionType actionType, String mitigationTemplate) {
        if (mitigationTemplate.equals(MitigationTemplate.Router_RateLimit_Route53Customer)) {
            if (actionType != null) {
                String msg = "ActionType must not be specified for mitigationTemplate: " + mitigationTemplate + ". ActionType is hard-coded for this template " +
                             "to be rate-limit with a fixed value. Instead found actionType: " + actionType;
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    private void validateMitigationConstraint(Constraint constraint, String mitigationTemplate) {
        // Step1. Check if it is a simple constraint, for Route53 single customer mitigations we don't allow composite constraints.
        if (!(constraint instanceof SimpleConstraint)) {
            // TODO - eventually support clients providing hosted-zone ids instead of destIPs.
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects a SimpleConstraint type, constraining only by destIPs, instead found: " + 
                         ReflectionToStringBuilder.toString(constraint);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        SimpleConstraint simpleConstraint = (SimpleConstraint) constraint;
        
        // Step2. Check if the simple constraint is for the expected attribute(s). Currently (03/27/2014) we only support constraining by DestinationIPs.
        // In future we should move to constraining by HostedZoneIds - which would be safer to guarantee we are indeed mitigation for a single customer.
        PacketAttributesEnumMapping constraintAttribute = null;
        try {
            constraintAttribute = PacketAttributesEnumMapping.valueOf(simpleConstraint.getAttributeName());
        } catch (IllegalArgumentException ex) {
            String msg = "Caught exception since the attribute in constraint is not recognizable, valid attribute names: " + PacketAttributesEnumMapping.values();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (constraintAttribute != PacketAttributesEnumMapping.DESTINATION_IP) {
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects a SimpleConstraint type, constraining only by attribute: " +
                         PacketAttributesEnumMapping.DESTINATION_IP.name() + " instead found: " + constraintAttribute.name();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // Step3. We currently (03/27/2014) only allow constraining by DestinationIPs. We thus check to ensure we have atleast 1 but not more than 4 of such destIPs specified.
        List<String> constraintValues = simpleConstraint.getAttributeValues();
        if (constraintValues.isEmpty() || (constraintValues.size() > 4)) {
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects at most 4 destIPs to constraint by, instead found: " + constraintValues.size() + 
                          " number of values - " + constraintValues;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // Step4. Ensure all the DestinationIPs are /32s
        for (String destIP : constraintValues) {
            String[] subnetParts = destIP.split(IPUtils.IP_CIDR_SEPARATOR);
            if ((subnetParts.length > 1) && (Integer.parseInt(subnetParts[1]) != IPUtils.NUM_BITS_IN_IPV4)) {
                String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs to be /32, instead found: " + constraintValues;
                   LOG.info(msg);
                   throw new IllegalArgumentException(msg);
            }
        }
        
        // Step5. Ensure all the DestinationIPs belong to Route53.
        String serviceName = serviceSubnetsMatcher.getServiceForSubnets(constraintValues);
        if ((serviceName == null) || !serviceName.equals(ServiceName.Route53)) {
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs in the constraint to belong to Route53, instead found: " + constraintValues;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(@Nonnull String templateForNewDefinition, @Nonnull String nameForNewDefinition, 
                                                        @Nonnull MitigationDefinition newDefinition, @Nonnull String templateForExistingDefinition, 
                                                        @Nonnull String nameForExistingDefinition, @Nonnull MitigationDefinition existingDefinition) {
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(nameForNewDefinition);
        Validate.notNull(newDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(nameForExistingDefinition);
        Validate.notNull(existingDefinition);
        
        if ((templateForNewDefinition.equals(MitigationTemplate.Router_RateLimit_Route53Customer)) && (templateForNewDefinition.equals(templateForExistingDefinition))) {
            String msg = "For MitigationTemplate: " + templateForNewDefinition + " we can have at most 1 mitigation active at a time. Currently mitigation: " + 
                         nameForExistingDefinition + " already exists for this template";
            LOG.info(msg);
            throw new DuplicateDefinitionException400(msg);
        }
    }
}
