package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.net.IPUtils;
import com.amazon.aws158.commons.net.IpCidr;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.google.common.collect.ImmutableList;

public class BlackholeArborCustomerValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(BlackholeArborCustomerValidator.class);
    
    @Override
    public void validateRequestForTemplate(
            MitigationModificationRequest request, String mitigationTemplate) {
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
        return ServiceName.Blackhole;
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate,
            DeviceNameAndScope deviceNameAndScope) {
        Validate.notNull(request);
        Validate.notEmpty(mitigationTemplate);
        Validate.notNull(deviceNameAndScope);
        
        if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            return;
        }
        
        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request);
            return;
        }
        
        if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request);
            return;
        }
        
        throw new IllegalArgumentException(
            String.format("request %s is Not supported for mitigation template %s", request, mitigationTemplate));
    }
    
    @Override
    public void validateCoexistenceForTemplateAndDevice(
            String templateForNewDefinition,
            String mitigationNameForNewDefinition,
            MitigationDefinition newDefinition,
            String templateForExistingDefinition,
            String mitigationNameForExistingDefinition,
            MitigationDefinition existingDefinition) {
       
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(mitigationNameForNewDefinition);
        Validate.notNull(newDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(mitigationNameForExistingDefinition);
        Validate.notNull(existingDefinition);
        
        // We do not current validate if there is a co-existed mitigation for the template and device
    }
    
    private static void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateArborLocation(request.getLocations());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private static void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateArborLocation(request.getLocation());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private static void validateMitigationConstraint(Constraint constraint) {
        // Step1. Check if it is a simple constraint
        if (!(constraint instanceof SimpleConstraint)) {
            throw new IllegalArgumentException("Expects a SimpleConstraint type, constraining only by destIPs, instead found: " + ReflectionToStringBuilder.toString(constraint));
        }
        
        SimpleConstraint simpleConstraint = (SimpleConstraint) constraint;
        // Step2. Check if the simple constraint is for the expected attribute(s). Currently we only support constraining by Destination IPs.
        PacketAttributesEnumMapping constraintAttribute = null;
        try {
            constraintAttribute = PacketAttributesEnumMapping.valueOf(simpleConstraint.getAttributeName());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Caught exception since the attributeName in constraint is not recognizable, valid attribute name: " + PacketAttributesEnumMapping.DESTINATION_IP);
        }
        
        if (constraintAttribute != PacketAttributesEnumMapping.DESTINATION_IP) {
            throw new IllegalArgumentException("Expects a SimpleConstraint type, constraining only by attribute: " + PacketAttributesEnumMapping.DESTINATION_IP.name() + " instead found: " + constraintAttribute.name());
        }
        
        // Step3. We currently only allow constraining by DestinationIPs. We thus check to ensure we have at least 1 of such destIPs specified.
        List<String> constraintValues = simpleConstraint.getAttributeValues();
        if ((constraintValues == null) || constraintValues.size() != 1) {
            throw new IllegalArgumentException("Expects 1 CIDR in the constraint, instead found: " + constraintValues.size());
        }
        
        // Step4. Ensure all the DestinationIPs are /32s
        for (String destCIDRStr : constraintValues) {
            // Explicit null-check despite the isBlank check later to provide specific message useful for system tests.
            if (StringUtils.isBlank(destCIDRStr)) {
                throw new IllegalArgumentException("CIDRs in the constraint must not be blank");
            }
            
            // Throws IllegalArgumentException if destCIDRStr is not a CIDR
            IpCidr destCIDR = IPUtils.parseIpCidr(destCIDRStr);
            if (!(destCIDR.getAddress() instanceof Inet4Address)) {
                throw new IllegalArgumentException("Only IPv4 addresses are currently supported");
            }
            
            if (destCIDR.getMask() != IPUtils.NUM_BITS_IN_IPV4) {
                throw new IllegalArgumentException("Blackholes can only work on /32s");
            }
            
            if (!IPUtils.isStandardAddress(destCIDR.getAddress())) {
                throw new IllegalArgumentException("Special (broadcast, multicast, etc) IP addresses are not supported");
            }
        }
    }
    
    private static void validateArborLocation(List<String> locationsToApplyMitigation) {
        if (!ImmutableList.of(StandardLocations.ARBOR).equals(locationsToApplyMitigation)) {
            String msg = "Blackhole mitigation must be applied to location " + StandardLocations.ARBOR;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateNoDeploymentChecks(List<MitigationDeploymentCheck> deploymentChecks) {
        if ((deploymentChecks != null) && !deploymentChecks.isEmpty()) {
            String msg = "Expect not have any deployment checks to be performed.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    } 
}
