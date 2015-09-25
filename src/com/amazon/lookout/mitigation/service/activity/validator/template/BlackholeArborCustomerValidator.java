package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.util.List;

import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.amazon.aws158.commons.net.IPUtils;
import com.amazon.aws158.commons.net.IpCidr;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

public class BlackholeArborCustomerValidator implements DeviceBasedServiceTemplateValidator {
    @Override
    public void validateRequestForTemplate(
            MitigationModificationRequest request, String mitigationTemplate) {
        Validate.notNull(request);
        Validate.notEmpty(mitigationTemplate);

        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(
            mitigationTemplate);
        if (deviceNameAndScope == null) {
            String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY +
                ": No DeviceNameAndScope mapping found for template: " + mitigationTemplate +
                ". Request being validated: " + ReflectionToStringBuilder.toString(request);
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
            MitigationModificationRequest request,
            String mitigationTemplate,
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
            String.format("request %s is not supported for mitigation template %s", request, mitigationTemplate));
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
        Validate.notEmpty(request.getMitigationName(), "mitigationName cannot be null or empty.");
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocations());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private static void validateEditRequest(EditMitigationRequest request) {
        Validate.notEmpty(request.getMitigationName(), "mitigationName cannot be null or empty.");
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocation());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private static void validateMitigationConstraint(Constraint constraint) {
        if (constraint == null) {
            throw new IllegalArgumentException("Constraint must not be null.");
        }

        if (!(constraint instanceof ArborBlackholeConstraint)) {
            throw new IllegalArgumentException("Expecting an ArborBlackholeConstraint type instead found: " +
                ReflectionToStringBuilder.toString(constraint));
        }

        ArborBlackholeConstraint arborConstraint = (ArborBlackholeConstraint) constraint;
        validateDestinationIP(arborConstraint.getIp());
    }

    private static void validateDestinationIP(String destCidrString) {
        if (StringUtils.isBlank(destCidrString)) {
            throw new IllegalArgumentException("CIDRs in the constraint must not be blank");
        }

        // Throws IllegalArgumentException if the argument is not a CIDR
        IpCidr destCidr = IPUtils.parseIpCidr(destCidrString);
        if (!(destCidr.getAddress() instanceof Inet4Address)) {
            throw new IllegalArgumentException("Only IPv4 addresses are currently supported");
        }

        if (destCidr.getMask() != IPUtils.NUM_BITS_IN_IPV4) {
            throw new IllegalArgumentException("Blackholes can only work on /32s");
        }

        if (!IPUtils.isStandardAddress(destCidr.getAddress())) {
            throw new IllegalArgumentException("Special (broadcast, multicast, etc) IP addresses are not supported");
        }
    }

    private static void validateNoLocations(List<String> locationsToApplyMitigation) {
        if ((locationsToApplyMitigation != null) &&
            !locationsToApplyMitigation.isEmpty()) {
            String msg = "Expect no locations to be provided in the request, since this mitigation is " +
                "expected to be applied to all Arbor devices.";
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateNoDeploymentChecks(List<MitigationDeploymentCheck> deploymentChecks) {
        if ((deploymentChecks != null) && !deploymentChecks.isEmpty()) {
            String msg = "Expect not have any deployment checks to be performed.";
            throw new IllegalArgumentException(msg);
        }
    } 
}
