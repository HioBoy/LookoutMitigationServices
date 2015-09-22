package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.amazon.aws158.commons.net.IPUtils;
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
import com.google.common.net.InetAddresses;

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
    
    private void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notEmpty(request.getMitigationName(), "mitigationName cannot be null or empty.");
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocations());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notEmpty(request.getMitigationName(), "mitigationName cannot be null or empty.");
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocation());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint());
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }
    
    private void validateMitigationConstraint(Constraint constraint) {
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

    private void validateDestinationIP(String destIP) {
        // Explicit null-check despite the isBlank check later to provide specific message useful for system tests.
        if (destIP == null) {
            throw new IllegalArgumentException(
                "Expects all destIPs to be /32, instead received one of the destIPs as null");
        }

        if (!StringUtils.isAsciiPrintable(destIP)) {
            throw new IllegalArgumentException("Invalid destIP found! A valid destIP name must conform to the " +
                "string representation of a /32 IPv4 address.");
        }

        if (StringUtils.isBlank(destIP)) {
            throw new IllegalArgumentException(
                "Expects all destIPs to be /32, instead received one of the destIPs as empty");
        }

        String[] subnetParts = destIP.split(IPUtils.IP_CIDR_SEPARATOR);
        if (subnetParts.length > 1) {
            int maskLength = 0;
            try {
                maskLength = Integer.parseInt(subnetParts[1]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Invalid destIP found! A valid destIP name must conform to the " +
                    "string representation of a /32 IPv4 address.");
            }

            if (maskLength != IPUtils.NUM_BITS_IN_IPV4) {
                throw new IllegalArgumentException("Expects all destIPs to be /32, instead found: " + destIP);
            }

            InetAddress address = InetAddresses.forString(subnetParts[0]);
            if (!(address instanceof Inet4Address)) {
                throw new IllegalArgumentException(
                    "Invalid destIP found! Only /32 IPv4 address are currently supported.");
            }

            if (address.isLoopbackAddress() ||
                address.isLinkLocalAddress() ||
                address.isMulticastAddress() ||
                address.isSiteLocalAddress()) {

                throw new IllegalArgumentException("Invalid destIP: " + subnetParts[0] + " is not a public IP address");
            }
        } else {
            throw new IllegalArgumentException("Invalid destIP found! A valid destIP name must conform to the " +
                "string representation of a /32 IPv4 address.");
        }
    }

    private void validateNoLocations(List<String> locationsToApplyMitigation) {
        if ((locationsToApplyMitigation != null) &&
            !locationsToApplyMitigation.isEmpty()) {
            String msg = "Expect no locations to be provided in the request, since this mitigation is " +
                "expected to be applied to all Arbor devices.";
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateNoDeploymentChecks(List<MitigationDeploymentCheck> deploymentChecks) {
        if ((deploymentChecks != null) && !deploymentChecks.isEmpty()) {
            String msg = "Expect not have any deployment checks to be performed.";
            throw new IllegalArgumentException(msg);
        }
    } 
}
