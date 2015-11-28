package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.net.Inet4Address;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.NonNull;

import com.amazon.arbor.ArborUtils;
import com.amazon.lookout.ddb.model.BlackholeDevice;
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
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
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
import com.google.common.collect.Sets;

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
    
    private static <T> List<T> getIntersection(Collection<? extends T> c1, Collection<? extends T> c2) {
        return c1.stream().filter(entry -> c2.contains(entry)).collect(Collectors.toList());
    }
    
    private Stream<String> getTransitProviderNames(Collection<String> transitProviderIds, TSDMetrics metrics) {
        return transitProviderIds.stream()
                .map(id -> blackholeMitigationHelper.loadTransitProvider(id, metrics)
                        .map(t -> t.getTransitProviderName())
                        .orElse("Unknown(" + id +")"));
    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(
            String templateForNewDefinition,
            String mitigationNameForNewDefinition,
            MitigationDefinition newDefinition,
            String templateForExistingDefinition,
            String mitigationNameForExistingDefinition,
            MitigationDefinition existingDefinition,
            TSDMetrics metrics) {

        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(mitigationNameForNewDefinition);
        Validate.notNull(newDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(mitigationNameForExistingDefinition);
        Validate.notNull(existingDefinition);
        
        if (!templateForExistingDefinition.equals(templateForNewDefinition)) return;
        
        ArborBlackholeConstraint existingConstraint = (ArborBlackholeConstraint) existingDefinition.getConstraint();
        ArborBlackholeConstraint newConstraint = (ArborBlackholeConstraint) newDefinition.getConstraint();
        
        // Only count existing enabled blackholes.
        // Note: We don't check the enabled flag for the new entry as even if its currently disabled we're 
        // probably going to be enabling it soon.
        if (!existingConstraint.isEnabled()) return;
        
        // Blackholes not on the same IP don't conflict
        if (!existingConstraint.getIp().equals(newConstraint.getIp())) return;
        String ip = existingConstraint.getIp();
        
        checkOverlappingTransitProviders(newConstraint, existingConstraint, mitigationNameForExistingDefinition, metrics);
        
        String existingCommunityString = blackholeMitigationHelper.getCommunityString(
                existingConstraint.getTransitProviderIds(), existingConstraint.getAdditionalCommunityString(), metrics);
        
        String newCommunityString = blackholeMitigationHelper.getCommunityString(
                newConstraint.getTransitProviderIds(), newConstraint.getAdditionalCommunityString(), metrics);
        
        checkOverlappingCommunityString(ip, newCommunityString, existingCommunityString, mitigationNameForExistingDefinition);
        
        checkOverlappingDevices(
                ip, newConstraint, newCommunityString, existingConstraint, existingCommunityString, 
                mitigationNameForExistingDefinition, metrics);
    }

    private void checkOverlappingDevices(String ip, ArborBlackholeConstraint newConstraint, String newCommunityString, ArborBlackholeConstraint existingConstraint, String existingCommunityString,
            String mitigationNameForExistingDefinition, TSDMetrics metrics) {
        List<BlackholeDevice> allDevices = blackholeMitigationHelper.listBlackholeDevices(metrics);
        
        List<String> existingDevices = BlackholeMitigationHelper.getDevicesForCommunity(allDevices, existingCommunityString).stream()
                .map(d -> d.getDeviceName()).collect(Collectors.toList());
        if (existingConstraint.getAdditionalRouters() != null) {
            existingDevices.addAll(existingConstraint.getAdditionalRouters());
        }
        
        List<String> newDevices = BlackholeMitigationHelper.getDevicesForCommunity(allDevices, newCommunityString).stream()
                .map(d -> d.getDeviceName()).collect(Collectors.toList());
        if (newConstraint.getAdditionalRouters() != null) {
            newDevices.addAll(newConstraint.getAdditionalRouters());
        }
        
        List<String> overlappingDevicesNames = getIntersection(existingDevices, newDevices);
        if (!overlappingDevicesNames.isEmpty()) {
            throw new DuplicateDefinitionException400(
                    String.format("Existing blackhole %s for ip %s would be deployed to overlapping devices %s",
                    mitigationNameForExistingDefinition, ip, overlappingDevicesNames));
        }
    }

    private static void checkOverlappingCommunityString(String ip, String newCommunityString, String existingCommunityString, String mitigationNameForExistingDefinition) {
        Set<String> existingSplitCommunityString = Sets.newHashSet(existingCommunityString.split(" "));
        Set<String> newSplitCommunityString = Sets.newHashSet(newCommunityString.split(" "));
        
        List<String> overlappingCommunities = getIntersection(existingSplitCommunityString, newSplitCommunityString);
        if (!overlappingCommunities.isEmpty()) {
            // The real problem is the devices but if the communities overlap so must the devices
            throw new DuplicateDefinitionException400(
                    String.format("Existing blackhole %s for ip %s has overlapping communities %s",
                    mitigationNameForExistingDefinition, ip, 
                    overlappingCommunities));
        }
    }

    private void checkOverlappingTransitProviders(
            ArborBlackholeConstraint newConstraint, ArborBlackholeConstraint existingConstraint, 
            String mitigationNameForExistingDefinition, TSDMetrics metrics) 
    {
        if (existingConstraint.getTransitProviderIds() != null && newConstraint.getTransitProviderIds() != null) {
            List<String> overlappingProviders = 
                    getIntersection(existingConstraint.getTransitProviderIds(), newConstraint.getTransitProviderIds());
            if (!overlappingProviders.isEmpty()) {
                throw new DuplicateDefinitionException400(
                        String.format("Existing blackhole %s for ip %s already exists with overlapping transit providers %s",
                        mitigationNameForExistingDefinition, existingConstraint.getIp(), 
                        getTransitProviderNames(overlappingProviders, metrics)
                            .collect(Collectors.joining(",", "[", "]"))));
            }
        }
    }

    private static void validateDeleteRequest(DeleteMitigationFromAllLocationsRequest request) {
        validateMitigationNameForDelete(request.getMitigationName());
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
        
        if (!mitigationName.startsWith(ArborConstants.MANAGED_BLACKHOLE_NAME_PREFIX) && 
            !mitigationName.startsWith(ArborConstants.SYSTEST_MANAGED_BLACKHOLE_NAME_PREFIX)) 
        {
            throw new IllegalArgumentException("Blackhole mitigationNames must start with " + ArborConstants.MANAGED_BLACKHOLE_NAME_PREFIX);
        }

        String error = ArborUtils.checkArgument(mitigationName);
        if (error != null) {
            throw new IllegalArgumentException(String.format("Invalid mitigationName: %s", error));
        }
    }
    
    /**
     * A version of validateMitigationName that allows names not starting with LKT- so that we can cleanup
     * mitigations created that were created before we enforced the prefix or that somehow bypassed the requirement.
     * 
     * @param mitigationName
     */
    private static void validateMitigationNameForDelete(String mitigationName) {
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
            
            validateTransitProviders(arborConstraint.getTransitProviderIds(), arborConstraint.getAdditionalCommunityString(), metrics);
        } else {
            throw new IllegalArgumentException(
                "Expecting an ArborBlackholeConstraint type, instead found: " +
                    ReflectionToStringBuilder.toString(constraint));
        }
    }
    
    private static final Pattern FIRST_ASN_PATTERN = Pattern.compile("^ *([0-9]+):[0-9]* ");
    
    private static int getFirstASN(String communityString) {
        Matcher matcher = FIRST_ASN_PATTERN.matcher(communityString);
        if (!matcher.find()) {
            throw new IllegalArgumentException(communityString + " is not a valid community string");
        }
        
        return Integer.parseInt(matcher.group(1));
    }
    
    /**
     * Check that the transit provider ids follow the right pattern, and that the match the actual
     * entries in the database.
     * 
     * @param transitProviderIds
     * @param metrics
     */
    private void validateTransitProviders(List<String> transitProviderIds, String additionalCommunityString, TSDMetrics metrics) {
        if (transitProviderIds == null || transitProviderIds.isEmpty()) return;
        int asn = -1;
        
        if (!StringUtils.isBlank(additionalCommunityString)) {
            asn = getFirstASN(additionalCommunityString);
        }
        
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
            
            if (!StringUtils.isBlank(provider.getTransitProviderCommunity())) {
                int newAsn = getFirstASN(provider.getTransitProviderCommunity());
                if (asn == -1) {
                    asn = newAsn;
                } else if (newAsn != asn) {
                    String msg; 
                    if (StringUtils.isBlank(additionalCommunityString)) {
                        msg = "All transit provider communities for a blackhole must share the same ASN.";
                    } else {
                        msg = "All entries in the additional community strign and the transit provider communities " + 
                              "for a blackhole must share the same ASN.";
                    }
                    throw new IllegalArgumentException(msg);
                }
            }
        }
        
        if (asn == -1) {
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
