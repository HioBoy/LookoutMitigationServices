package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;

import lombok.NonNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.net.IPUtils;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;

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
    
    private final List<String> validPacketAttributeValues;
    private final DataConverter jsonDataConverter = new JsonDataConverter();
    
    public Route53SingleCustomerMitigationValidator(@NonNull ServiceSubnetsMatcher serviceSubnetsMatcher) {
        this.serviceSubnetsMatcher = serviceSubnetsMatcher;
        
        this.validPacketAttributeValues = new ArrayList<>();
        for (PacketAttributesEnumMapping packetAttr : PacketAttributesEnumMapping.values()) {
            validPacketAttributeValues.add(packetAttr.name());
        }
    }
    
    @Override
    public void validateRequestForTemplate(
            @NonNull MitigationModificationRequest request, @NonNull String mitigationTemplate, @NonNull TSDMetrics metrics) 
    {
        Validate.notEmpty(mitigationTemplate);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
        if (deviceNameAndScope == null) {
            String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY + ": No DeviceNameAndScope mapping found for template: " + 
                         mitigationTemplate + ". Request being validated: " + ReflectionToStringBuilder.toString(request);
            LOG.warn(msg);
            throw new InternalServerError500(msg);
        }
        
        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope, metrics);
    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Route53;
    }

    @Override
    public void validateRequestForTemplateAndDevice(@NonNull MitigationModificationRequest request, @NonNull String mitigationTemplate,
                                                    @NonNull DeviceNameAndScope deviceNameAndScope, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);

        String mitigationName = request.getMitigationName();
        
        MitigationDefinition mitigationDefinition = null;
        List<String> locationsToApplyMitigation = null;
        // Extract mitigationDefinition and locations from Create/Edit Mitigation Requests only.
        if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            return;
        } else if (request instanceof CreateMitigationRequest) {
            locationsToApplyMitigation = ((CreateMitigationRequest) request).getLocations();
            mitigationDefinition = ((CreateMitigationRequest) request).getMitigationDefinition();
        } else {
            throw new IllegalArgumentException(
                    String.format("Mitigation template %s can not be modified, only created and deleted", mitigationTemplate));
        }
        
        Constraint mitigationConstraint = mitigationDefinition.getConstraint();
        ActionType mitigationAction = mitigationDefinition.getAction();
        
        validateMitigationName(mitigationName, mitigationTemplate);
        validateLocationsToApply(locationsToApplyMitigation, mitigationTemplate);
        validateActionType(mitigationAction, mitigationTemplate);
        validateMitigationConstraint(mitigationConstraint, mitigationTemplate);
        validatePreDeploymentChecks(request.getPreDeploymentChecks(), mitigationTemplate);
        validatePostDeploymentChecks(request.getPostDeploymentChecks(), mitigationTemplate);
    }
    
    private void validateMitigationName(String mitigationName, String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            validateRouterMitigationName(mitigationName, mitigationTemplate);
            break;
        case MitigationTemplate.Router_CountMode_Route53Customer:
            validateRouterMitigationName(mitigationName, mitigationTemplate);
            break;
        default:
            throw new IllegalArgumentException("Mitigation template is not supported: " + mitigationTemplate);
        }            
    }
    
    private void validateRouterMitigationName(String mitigationName, String mitigationTemplate) {
        Matcher matcher = INVALID_ROUTER_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        if (matcher.find()) {
            String msg = "Invalid mitigationName + " + mitigationName + " found for template: " + mitigationTemplate + ". Name cannot contain any control characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateLocationsToApply(List<String> locationsToApplyMitigation, String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            checkForNoLocations(locationsToApplyMitigation, mitigationTemplate);
            break;
        case MitigationTemplate.Router_CountMode_Route53Customer:
            checkForNoLocations(locationsToApplyMitigation, mitigationTemplate);
            break;
        default:
            throw new IllegalArgumentException("Mitigation template is not supported: " + mitigationTemplate);
        }
    }

    private void checkForNoLocations(List<String> locationsToApplyMitigation, String mitigationTemplate) {
        if ((locationsToApplyMitigation != null) && !locationsToApplyMitigation.isEmpty()) {
            String msg = "For the template: " + mitigationTemplate + " no locations are expected to be provided in the request, since this mitigation is " +
                        "expected to be applied to all non-Blackwatch POPs.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    
    private void validateActionType(ActionType actionType, String mitigationTemplate) {
        if (mitigationTemplate.equals(MitigationTemplate.Router_RateLimit_Route53Customer)) {
            checkForNoAction(actionType, mitigationTemplate);
        }
        
        if (mitigationTemplate.equals(MitigationTemplate.Router_CountMode_Route53Customer)) {
            checkForNoAction(actionType, mitigationTemplate);
        }
    }

    private void checkForNoAction(ActionType actionType, String mitigationTemplate) {
        if (actionType != null) {
            String msg = "ActionType must not be specified for mitigationTemplate: " + mitigationTemplate + ", instead found : " + actionType;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
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
        } catch (Exception ex) {
            String msg = "Caught exception since the attributeName in constraint is not recognizable, valid attribute names: " + validPacketAttributeValues;
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
        if ((constraintValues == null) || constraintValues.isEmpty() || (constraintValues.size() > 4)) {
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects at least 1 and at most 4 destIPs in the constraint, instead found: " + constraintValues;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // Step4. Ensure all the DestinationIPs are /32s
        for (String destIP : constraintValues) {
            // Explicit null-check despite the isBlank check later to provide specific message useful for system tests.
            if (destIP == null) {
                String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs to be /32, instead received one of the destIPs as null";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            if (!StringUtils.isAsciiPrintable(destIP)) {
                String msg = "Invalid destIP found! A valid destIP name must conform to the string representation of an IPv4 address.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            if (StringUtils.isBlank(destIP)) {
                String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs to be /32, instead received one of the destIPs as empty";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            String[] subnetParts = destIP.split(IPUtils.IP_CIDR_SEPARATOR);
            if (subnetParts.length > 1) {
                int maskLength = 0;
                try {
                    maskLength = Integer.parseInt(subnetParts[1]);
                } catch (NumberFormatException ex) {
                    String msg = "Invalid destIP found! A valid destIP name must conform to the string representation of an IPv4 address.";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
                
                if (maskLength != IPUtils.NUM_BITS_IN_IPV4) {
                    String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs to be /32, instead found: " + constraintValues;
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
            }
        }
        
        // Step5. Ensure there are no duplicates in the destIPs.
        Set<String> destIPs = Sets.newHashSet(constraintValues);
        if (destIPs.size() != constraintValues.size()) {
            String msg = "Found one or more duplicate destIPs in constraints: " + constraintValues;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // Step6. Ensure all the DestinationIPs belong to Route53.
        String serviceName = serviceSubnetsMatcher.getServiceForSubnets(constraintValues);
        if ((serviceName == null) || !serviceName.equals(ServiceName.Route53)) {
            String msg = "MitigationTemplate: " + mitigationTemplate + " expects all destIPs in the constraint to belong to Route53, instead found: " + constraintValues;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(@NonNull String templateForNewDefinition, @NonNull String nameForNewDefinition,
                                                        @NonNull MitigationDefinition newDefinition, @NonNull String templateForExistingDefinition,
                                                        @NonNull String nameForExistingDefinition, @NonNull MitigationDefinition existingDefinition,
                                                        @NonNull TSDMetrics metrics) {
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(nameForNewDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(nameForExistingDefinition);
        
        if (templateForNewDefinition.equals(MitigationTemplate.Router_RateLimit_Route53Customer)) {
            checkForDuplicateDefinition(templateForNewDefinition, nameForNewDefinition, newDefinition, templateForExistingDefinition, nameForExistingDefinition, existingDefinition);
        }
        
        if (templateForNewDefinition.equals(MitigationTemplate.Router_CountMode_Route53Customer)) {
            checkForDuplicateDefinition(templateForNewDefinition, nameForNewDefinition, newDefinition, templateForExistingDefinition, nameForExistingDefinition, existingDefinition);
        }
    }

    /**
     * We currently check if 2 definitions are exactly identical. There could be cases where 2 definitions are equivalent, but not identical (eg:
     * when they have the same constraints, but the constraints are in different order) - in those cases we don't treat them as identical for now.
     * We could do so, by enforcing a particular ordering to the mitigation definitions when we persist the definition - however it might get tricky
     * to do so for different use-cases, eg: for IPTables maybe the user crafted rules such that they are in a certain order for a specific reason.
     * We could have a deeper-check based on the template - which checks if 2 mitigations are equivalent, but we don't have a strong use-case for such as of now, 
     * hence keeping the comparison simple for now.
     * 
     * We also first check if the hashcode for definitions matches - which acts as a shortcut to avoid deep inspection of the definitions.
     */
    private void checkForDuplicateDefinition(@NonNull String templateForNewDefinition, @NonNull String nameForNewDefinition,
                                             @NonNull MitigationDefinition newDefinition, @NonNull String templateForExistingDefinition,
                                             @NonNull String nameForExistingDefinition, @NonNull MitigationDefinition existingDefinition) {
        if ((existingDefinition.hashCode()== newDefinition.hashCode()) && newDefinition.equals(existingDefinition)) {
            String msg = "Found identical mitigation definition: " + nameForExistingDefinition + " for existingTemplate: " + templateForExistingDefinition +
                         " with definition: " + jsonDataConverter.toData(existingDefinition) + " for request with MitigationName: " + nameForNewDefinition + 
                         " and MitigationTemplate: " + templateForNewDefinition + " with definition: " + jsonDataConverter.toData(newDefinition);
            LOG.info(msg);
            throw new DuplicateDefinitionException400(msg);
        }
        
        if (templateForNewDefinition.equals(templateForExistingDefinition)) {
            String msg = "For MitigationTemplate: " + templateForNewDefinition + " we can have at most 1 mitigation active at a time. Currently mitigation: " + 
                         nameForExistingDefinition + " already exists for this template";
            LOG.info(msg);
            throw new DuplicateDefinitionException400(msg);
        }
    }
    
    private void validatePreDeploymentChecks(@Nullable List<MitigationDeploymentCheck> preDeploymentChecks, @NonNull String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            validateNoDeploymentChecks(preDeploymentChecks, mitigationTemplate);
            break;
        case MitigationTemplate.Router_CountMode_Route53Customer:
            validateNoDeploymentChecks(preDeploymentChecks, mitigationTemplate);
            break;
        default:
            throw new IllegalArgumentException("Mitigation template is not supported: " + mitigationTemplate);
        }
    }
    
    private void validatePostDeploymentChecks(@Nullable List<MitigationDeploymentCheck> postDeploymentChecks, @NonNull String mitigationTemplate) {
        switch (mitigationTemplate) {
        case MitigationTemplate.Router_RateLimit_Route53Customer:
            validateNoDeploymentChecks(postDeploymentChecks, mitigationTemplate);
            break;
        case MitigationTemplate.Router_CountMode_Route53Customer:
            validateNoDeploymentChecks(postDeploymentChecks, mitigationTemplate);
            break;
        default:
            throw new IllegalArgumentException("Mitigation template is not supported: " + mitigationTemplate);
        }
    }
    
    private void validateNoDeploymentChecks(List<MitigationDeploymentCheck> deploymentChecks, String mitigationTemplate) {
        if ((deploymentChecks != null) && !deploymentChecks.isEmpty()) {
            String msg = "For MitigationTemplate: " + mitigationTemplate + " we expect to not have any deployment checks to be performed.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }    
}
