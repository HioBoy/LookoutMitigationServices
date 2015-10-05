package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.util.List;

import com.amazon.arbor.ArborUtils;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import com.amazon.lookout.mitigation.service.ArborBlackholeSetEnabledStateConstraint;
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
            validateDeleteRequest((DeleteMitigationFromAllLocationsRequest) request);
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

    private void validateDeleteRequest(DeleteMitigationFromAllLocationsRequest request) {
        validateMitigationName(request.getMitigationName());
    }

    private static void validateCreateRequest(CreateMitigationRequest request) {
        validateMitigationName(request.getMitigationName());
        validateDescription(request.getMitigationActionMetadata().getDescription());
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocations());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }

    private static void validateEditRequest(EditMitigationRequest request) {
        validateMitigationName(request.getMitigationName());
        validateDescription(request.getMitigationActionMetadata().getDescription());
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocation());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }

    private static void validateMitigationName(String mitigationName) {
        if (StringUtils.isBlank(mitigationName)) {
            throw new IllegalArgumentException("mitigationName cannot be null or empty");
        }

        String error = ArborUtils.checkArgument(mitigationName);
        if (error != null) {
            throw new IllegalArgumentException(String.format("Invalid mitigationName: %s", error));
        }
    }

    private static void validateDescription(String description) {
        if (description == null) {
            return;
        }

        String error = ArborUtils.checkArgument(description);
        if (error != null) {
            throw new IllegalArgumentException(String.format("Invalid description: %s", error));
        }
    }

    private static void validateMitigationConstraint(Constraint constraint) {
        if (constraint == null) {
            throw new IllegalArgumentException("Constraint must not be null.");
        }

        if (constraint instanceof ArborBlackholeConstraint) {
            ArborBlackholeConstraint arborConstraint = (ArborBlackholeConstraint) constraint;
            validateDestinationIP(arborConstraint.getIp());
        } else if (!(constraint instanceof ArborBlackholeSetEnabledStateConstraint)) {
            throw new IllegalArgumentException(
                "Expecting an ArborBlackholeConstraint or ArborBlackholeSetEnabledStateConstraint type, " +
                    "instead found: " +
                    ReflectionToStringBuilder.toString(constraint));
        }
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
