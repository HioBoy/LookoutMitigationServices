package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;

import com.amazon.arbor.ArborUtils;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.arbor.model.ArborConstants;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.amazon.aws158.commons.metric.TSDMetrics;
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
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;

public class BlackholeArborCustomerValidator implements DeviceBasedServiceTemplateValidator {
    @NonNull 
    private final BlackholeMitigationHelper blackholeMitigationHelper;
    
    public BlackholeArborCustomerValidator(@NonNull BlackholeMitigationHelper blackholeMitigationHelper) {
        this.blackholeMitigationHelper = blackholeMitigationHelper;
    }

    @Override
    public void validateRequestForTemplate(
            @NonNull MitigationModificationRequest request, @NonNull  String mitigationTemplate, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);

        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(
            mitigationTemplate);
        if (deviceNameAndScope == null) {
            String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY +
                ": No DeviceNameAndScope mapping found for template: " + mitigationTemplate +
                ". Request being validated: " + ReflectionToStringBuilder.toString(request);
            throw new InternalServerError500(msg);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope, metrics);
    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Blackhole;
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            @NonNull MitigationModificationRequest request,
            @NonNull String mitigationTemplate,
            @NonNull DeviceNameAndScope deviceNameAndScope,
            @NonNull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);
        
        if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            validateDeleteRequest((DeleteMitigationFromAllLocationsRequest) request);
            return;
        }
        
        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request, metrics);
            return;
        }
        
        if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request, metrics);
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

    private static void validateDeleteRequest(DeleteMitigationFromAllLocationsRequest request) {
        validateMitigationName(request.getMitigationName());
    }

    private void validateCreateRequest(@NonNull CreateMitigationRequest request, @NonNull TSDMetrics metrics) {
        validateMitigationName(request.getMitigationName());
        validateDescription(request.getMitigationActionMetadata().getDescription());
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocations());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint(), metrics);
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }

    private void validateEditRequest(@NonNull EditMitigationRequest request, @NonNull TSDMetrics metrics) {
        validateMitigationName(request.getMitigationName());
        validateDescription(request.getMitigationActionMetadata().getDescription());
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateNoLocations(request.getLocation());
        validateMitigationConstraint(request.getMitigationDefinition().getConstraint(), metrics);
        validateNoDeploymentChecks(request.getPreDeploymentChecks());
        validateNoDeploymentChecks(request.getPostDeploymentChecks());
    }

    private static void validateMitigationName(String mitigationName) {
        if (StringUtils.isBlank(mitigationName)) {
            throw new IllegalArgumentException("mitigationName cannot be null or empty");
        }
        
        if (!mitigationName.startsWith(ArborConstants.MANAGED_BLACKHOLE_NAME_PREFIX)) {
            throw new IllegalArgumentException("Blackhole mitigationNames must start with " + ArborConstants.MANAGED_BLACKHOLE_NAME_PREFIX);
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

    private void validateMitigationConstraint(Constraint constraint, TSDMetrics metrics) {
        if (constraint == null) {
            throw new IllegalArgumentException("Constraint must not be null.");
        }

        if (constraint instanceof ArborBlackholeConstraint) {
            ArborBlackholeConstraint arborConstraint = (ArborBlackholeConstraint) constraint;
            validateDestinationIP(arborConstraint.getIp());
            
            boolean hasAdditionalCommunityString =
                    !StringUtils.isEmpty(arborConstraint.getAdditionalCommunityString());
            
            if (!hasAdditionalCommunityString && 
               (arborConstraint.getTransitProviderIds() == null || arborConstraint.getTransitProviderIds().isEmpty()))
            {
                throw new IllegalArgumentException("At least one transit provider or additionalCommunityString must be provided");
            }
            
            if (hasAdditionalCommunityString) {
                RequestValidator.validateCommunityString(arborConstraint.getAdditionalCommunityString());
            }
            
            validateTransitProviders(arborConstraint.getTransitProviderIds(), hasAdditionalCommunityString, metrics);
        } else {
            throw new IllegalArgumentException(
                "Expecting an ArborBlackholeConstraint type, instead found: " +
                    ReflectionToStringBuilder.toString(constraint));
        }
    }
    
    /**
     * Check that the transit provider ids follow the right pattern, and that the match the actual
     * entries in the database.
     * 
     * @param transitProviderIds
     * @param metrics
     */
    private void validateTransitProviders(List<String> transitProviderIds, boolean hasAdditionalCommunityString, TSDMetrics metrics) {
        if (transitProviderIds == null || transitProviderIds.isEmpty()) return;
        boolean hasCommunityString = hasAdditionalCommunityString;
        
        // First validate they at least follow the expected pattern
        transitProviderIds.forEach(id -> RequestValidator.validateTransitProviderId(id));
        
        // The number of transit providers should be small so its cheaper to fetch them all in one go then 
        // to fetch one by one
        Map<String, TransitProvider> transitProviders = blackholeMitigationHelper.listTransitProviders(metrics).stream()
                    .collect(Collectors.toMap(t -> t.getTransitProviderId(), t -> t));
        
        for (String id : transitProviderIds) {
            TransitProvider provider = transitProviders.get(id);
            if (provider == null) {
                throw new IllegalArgumentException("Invalid transit provider id: " + id);
            }
            
            if (!StringUtils.isEmpty(provider.getTransitProviderCommunity())) {
                hasCommunityString = true;
            }
        }
        
        if (!hasCommunityString) {
            throw new IllegalArgumentException(
                    "None of the specified transit providers has a community string " + 
                    "and no additional community string was provided.");
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
