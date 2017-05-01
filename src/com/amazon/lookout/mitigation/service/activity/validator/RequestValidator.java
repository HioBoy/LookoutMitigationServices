package com.amazon.lookout.mitigation.service.activity.validator;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.blackwatch.location.state.model.LocationType;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchMitigationResourceType;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.BlackholeDeviceInfo;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.CreateBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.CreateTransitProviderRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.GetBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.GetTransitProviderRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.TransitProviderInfo;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.UpdateTransitProviderRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateRequest;
import com.amazon.lookout.mitigation.service.activity.GetLocationDeploymentHistoryActivity;
import com.amazon.lookout.mitigation.service.activity.GetMitigationHistoryActivity;
import com.amazon.lookout.mitigation.service.activity.ListBlackWatchMitigationsActivity;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
/**
 * RequestValidator is a basic validator for requests to ensure the requests are well-formed and contain all the required inputs.
 * This validator performs template-agnostic validation - it doesn't dive deep into specific requirements of each template, the TemplateBasedRequestValidator
 * is responsible for such checks.  
 *
 */
@ThreadSafe
public class RequestValidator {
    private static final Log LOG = LogFactory.getLog(RequestValidator.class);
    
    // Maintain a Set of the mitigationTemplate, making it easier to search if a single template is part of the set of defined templates.
    private final Set<String> mitigationTemplates = Sets.newHashSet(MitigationTemplate.values());
    
    // Maintain a Set of the supported serviceNames, making it easier to search if a single serviceName is part of the set of defined serviceNames.
    private final Set<String> serviceNames = Sets.newHashSet(ServiceName.values());
    
    private static final int DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS = 100;
    
    private static final int DEFAULT_MAX_LENGTH_MITIGATION_ID = 100;
    private static final int DEFAULT_MAX_LENGTH_RESOURCE_ID = 50;
    private static final int DEFAULT_MAX_LENGTH_RESOURCE_TYPE = 20;
    private static final int DEFAULT_MAX_LENGTH_OWNER_ARN = 100;
    //Thirty days max
    private static final int MAX_MINUTES_TO_LIVE_DAYS = 30;
    private static final int MAX_MINUTES_TO_LIVE = MAX_MINUTES_TO_LIVE_DAYS*24*60;

    // Some nominal minimum PPS limit > 0.  It will be enforced on ALL shapers when
    // more than just the default shaper is used.
    private static final long MIN_PPS = 10L;

    //50 locations at 320Gbps per
    private static final long MAX_BPS = (long) (320L * 1e9 * 50);
    //84Bytes is the min frame size including preamble and interframe gap.
    private static final long MIN_FRAME_BITS = 8*84;
    //10Gbps = 14.88Mpps
    private static final long MAX_PPS = MAX_BPS/MIN_FRAME_BITS;
    
    private static final int DEFAULT_MAX_LENGTH_DESCRIPTION = 500;
    private static final int MAX_LENGTH_BLACKHOLE_DEVICE = 50;
    
    private static final int MAX_NUMBER_OF_LOCATIONS = 200;
    private static final int MAX_NUMBER_OF_TICKETS = 10;

    private static final String DEFAULT_SHAPER_NAME = "default";
    
    private static final RecursiveToStringStyle recursiveToStringStyle = new RecursiveToStringStyle();
    
    private final Set<String> deviceNames;
    private final Set<String> deviceScopes;
    private final ServiceLocationsHelper serviceLocationsHelper;
    private final EdgeLocationsHelper edgeLocationsHelper;
    private final BlackWatchBorderLocationValidator blackWatchBorderLocationValidator;
    private final BlackWatchEdgeLocationValidator blackWatchEdgeLocationValidator;
    
    @ConstructorProperties({"serviceLocationsHelper", "edgeLocationsHelper", "blackWatchBorderLocationValidator",
        "blackWatchEdgeLocationValidator"}) 
    public RequestValidator(@NonNull ServiceLocationsHelper serviceLocationsHelper,
            @NonNull EdgeLocationsHelper edgeLocationsHelper,
            @NonNull BlackWatchBorderLocationValidator blackWatchBorderLocationValidator,
            @NonNull BlackWatchEdgeLocationValidator blackWatchEdgeLocationValidator) {
        this.serviceLocationsHelper = serviceLocationsHelper;
        this.edgeLocationsHelper = edgeLocationsHelper;
        this.blackWatchBorderLocationValidator = blackWatchBorderLocationValidator;
        this.blackWatchEdgeLocationValidator = blackWatchEdgeLocationValidator;
        
        this.deviceNames = new HashSet<>();
        for (DeviceName deviceName : DeviceName.values()) {
            deviceNames.add(deviceName.name());
        }
        
        this.deviceScopes = new HashSet<>();
        for (DeviceScope deviceScope : DeviceScope.values()) {
            deviceScopes.add(deviceScope.name());
        }
    }

    /**
     * Validates if the request object passed to the CreateMitigationAPI is valid.
     * @param request MitigationModificationRequest representing the input to the CreateMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateCreateRequest(@NonNull CreateMitigationRequest request) {
        validateCommonModificationRequestParameters(request);
        
        if (request.getMitigationDefinition() == null) {
            String msg = "No mitigation definition found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We only check for the constraint and not the action, since for some templates, it is valid for requests to not have any actions.
        // Template-based validations should take care of specific checks related to their template.
        if (request.getMitigationDefinition().getConstraint() == null) {
            String msg = "No constraint found for mitigation definition.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the EditMitigationAPI is valid.
     * @param request MitigationModificationRequest representing the input to the EditMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateEditRequest(@NonNull EditMitigationRequest request) {
        validateCommonModificationRequestParameters(request);
        
        if (request.getMitigationDefinition() == null) {
            String msg = "No mitigation definition found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We only check for the constraint and not the action, since for some templates, it is valid for requests to not have any actions.
        // Template-based validations should take care of specific checks related to their template.
        if (request.getMitigationDefinition().getConstraint() == null) {
            String msg = "No constraint found for mitigation definition.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (request.getMitigationVersion() <= 1) {
            String msg = "Version of the mitigation to be edited should be set to > 1, instead found: " + request.getMitigationVersion();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Validates if the request object passed to the
     * DeactivateBlackWatchMitigation is valid.
     * @param request Instance of DeactivateBlackWatchMitigationRequest representing the input to the DeactivateBlackWatchMitigation.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateDeactivateBlackWatchMitigationRequest(@NonNull DeactivateBlackWatchMitigationRequest request) {
        validateUserName(request.getMitigationActionMetadata().getUser());
        validateToolName(request.getMitigationActionMetadata().getToolName());
        validateMitigationId(request.getMitigationId());
    }
    
    public void validateChangeBlackWatchMitigationOwnerARNRequest(@NonNull ChangeBlackWatchMitigationOwnerARNRequest request) {
        validateUserName(request.getMitigationActionMetadata().getUser());
        validateToolName(request.getMitigationActionMetadata().getToolName());
        validateMitigationDescription(request.getMitigationActionMetadata().getDescription());
        validateMitigationId(request.getMitigationId());
        validateUserARN(request.getNewOwnerARN());
        validateUserARN(request.getExpectedOwnerARN());
    }
    
    public void validateListBlackWatchMitigationsRequest(@NonNull ListBlackWatchMitigationsRequest request) {
        MitigationActionMetadata actionMetadata = request.getMitigationActionMetadata();
        if (actionMetadata == null) {
            String msg = "No MitigationActionMetadata found! Passing the MitigationActionMetadata is mandatory for list blackwatch mitigation.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateUserName(actionMetadata.getUser());
        validateToolName(actionMetadata.getToolName());
        validateMitigationDescription(actionMetadata.getDescription());        
        
        String mitigationId = request.getMitigationId();
        String resourceId = request.getResourceId();
        String resourceType = request.getResourceType();
        String ownerARN = request.getOwnerARN();
        if (mitigationId != null) {
            validateMitigationId(mitigationId);
        }
        
        if (resourceId != null) {
            validateResourceId(resourceId);
        }
        
        if (resourceType != null) {
            validateResourceType(resourceType);
        }
        
        if (ownerARN != null) {
            validateUserARN(ownerARN);
        }
        
        Long maxNumberOfEntriesToFetch = request.getMaxResults();
        Long maxEntryCount = ListBlackWatchMitigationsActivity.MAX_NUMBER_OF_ENTRIES_TO_FETCH;
        if (maxNumberOfEntriesToFetch != null) {
            validMaxNumberOfEntriesToFetch(maxNumberOfEntriesToFetch, 0, maxEntryCount);
        }

    }
    
    /**
     * Validates if the request object passed to the DeleteMitigationAPI is valid.
     * @param request Instance of DeleteMitigationFromAllLocationsRequest representing the input to the DeleteMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateDeleteRequest(@NonNull DeleteMitigationFromAllLocationsRequest request) {
        validateCommonModificationRequestParameters(request);
        
        if (request.getMitigationVersion() < 1) {
            String msg = "Version of the mitigation to be deleted should be set to >=1, instead found: " + request.getMitigationVersion();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the GetRequestStatus API is valid
     * @param A GetRequestStatusRequest object representing the input to the GetRequestStatus API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetRequestStatusRequest(@NonNull GetRequestStatusRequest request) {
        if (request.getJobId() < 1) {
            String msg = "Job IDs should be >= 1, instead found: " + request.getJobId();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateMitigationTemplate(request.getMitigationTemplate());
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
    }

    /**
     * Validates if the request object passed to the AbortDeployment API is valid
     * @param A AbortDeploymentRequest object representing the input to the AbortDeployment API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateAbortDeploymentRequest(@NonNull AbortDeploymentRequest request) {
        if (request.getJobId() < 1) {
            String msg = "Job IDs should be >= 1, instead found: " + request.getJobId();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateBlackWatchMitigationTemplate(request.getMitigationTemplate());
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
    }
    
    /**
     * Validates if the request object passed to the GetMitigationInfo API is valid
     * @param An instance of GetMitigationInfoRequest representing the input to the GetMitigationInfo API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationInfoRequest(@NonNull GetMitigationInfoRequest request) {
        validateDeviceScope(request.getDeviceScope());
        validateMitigationName(request.getMitigationName());
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
    }
    
    /**
     * Validates if the request object passed to the GetMitigationHistory API is valid
     * @param An instance of GetMitigationHistoryRequest representing the input to the GetMitigationHistory API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationHistoryRequest(
            GetMitigationHistoryRequest request) {
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
        validateDeviceScope(request.getDeviceScope());
        validateMitigationName(request.getMitigationName());
        Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
        int maxHistoryEntryCount = GetMitigationHistoryActivity.MAX_NUMBER_OF_HISTORY_TO_FETCH;
        if (maxNumberOfHistoryEntriesToFetch != null) {
            Validate.isTrue((maxNumberOfHistoryEntriesToFetch > 0) &&
                    (maxNumberOfHistoryEntriesToFetch <= maxHistoryEntryCount),
                    String.format("maxNumberOfHistoryEntriesToFetch should be larger than 0, and <= %s", maxHistoryEntryCount));
        }
        Integer startVersion = request.getExclusiveStartVersion();
        if (startVersion != null) {
            Validate.isTrue(startVersion > 1, "exclusiveStartVersion number should be larger than 1");
        }
    }

    /**
     * Validates if the request object passed to the GetMitigationHistory API is valid
     * @param An instance of GetMitigationHistoryRequest representing the input to the GetMitigationHistory API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetLocationDeploymentHistoryRequest(GetLocationDeploymentHistoryRequest request) {
        validateLocation(request.getLocation());
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
        validateLocationOnDeviceAndService(DeviceName.valueOf(request.getDeviceName()),
                request.getServiceName(), request.getLocation());
        Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
        int maxHistoryEntryCount = GetLocationDeploymentHistoryActivity.MAX_NUMBER_OF_HISTORY_TO_FETCH;
        if (maxNumberOfHistoryEntriesToFetch != null) {
            Validate.isTrue((maxNumberOfHistoryEntriesToFetch > 0) && 
                    (maxNumberOfHistoryEntriesToFetch <= maxHistoryEntryCount),
                    String.format("maxNumberOfHistoryEntriesToFetch should be larger than 0, and <= %d", maxHistoryEntryCount));
        }
        Long lastEvaluatedTimestamp = request.getExclusiveLastEvaluatedTimestamp();
        if (lastEvaluatedTimestamp != null) {
            Validate.isTrue(lastEvaluatedTimestamp > 0, "exclusiveLastEvaluatedTimestamp should be larger than 0");
        }
    }

    /**
     * Validate get mitigation definition request.
     * @param request : GetMitigationDefinitionRequest
     * throw IllegalArgumentException if it is invalid
     */
    public void validateGetMitigationDefinitionRequest(GetMitigationDefinitionRequest request) {
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
        validateMitigationName(request.getMitigationName());
        Validate.isTrue(request.getMitigationVersion() > 0, "mitigationVersion should be larger than 0.");
    }

    /**
     * Validate rollback mitigation request.
     * @param request : rollback mitigation request
     * @throws IllegalArgumentException if it is invalid
     */
    public void validateRollbackRequest(RollbackMitigationRequest request) {
        validateCommonModificationRequestParameters(request);
        Validate.isTrue(request.getRollbackToMitigationVersion() > 0,
                "rollback to mitigation version should be larger than 0");
    }
    
    /**
     * Validates if the request object passed to the GetLocationHostStatus API is valid
     * @param An instance of GetLocationHostStatusRequest representing the input to the GetLocationHostStatus API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetLocationHostStatusRequest(GetLocationHostStatusRequest request) {
        validateLocation(request.getLocation());
    }

    /**
     * Validates if the request object passed to the ListBlackWatchLocationState API is valid
     * @param An instance of ListBlackWatchLocationStateRequest representing the input to the ListBlackWatchLocationState API
     * @throws IllegalArgumentException
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateListBlackWatchLocationsRequest(ListBlackWatchLocationsRequest request) {
        String region = request.getRegion();
        if (region != null) {
            try {
                Regions.fromName(region);
            } catch (IllegalArgumentException ex) {
                String msg = String.format("Invalid region name - %s", region);
                LOG.info(msg, ex);
                throw new IllegalArgumentException(msg);
            }
        }
    }

    /**
     * Validates if the request object passed to the UpdateBlackWatchLocationState API is valid
     * @param An instance of UpdateBlackWatchLocationStateRequest representing the input to the UpdateBlackWatchLocationState API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateUpdateBlackWatchLocationStateRequest(UpdateBlackWatchLocationStateRequest request) {
        validateLocation(request.getLocation());
        validateChangeReason(request.getReason());
        validateLocationType(request.getLocationType());
    }
    
    /**
     * Validate the value in the rollback request match the one in system.
     * Also validate rollback to version is not delete request
     * @param request : rollback reqeust
     * @param mitigationRequestDescription : mitigation request description,
     *      which is retrieved from system request storage
     */
    public void validateRollbackRequest(RollbackMitigationRequest request,
            MitigationRequestDescription mitigationRequestDescription) {
        Validate.isTrue(mitigationRequestDescription.getMitigationTemplate().equals(request.getMitigationTemplate()),
                String.format("mitigationTemplate %s in request does not match %s in system, request : %s",
                        request.getMitigationTemplate(), mitigationRequestDescription.getMitigationTemplate(),
                        ReflectionToStringBuilder.toString(request, recursiveToStringStyle)));

        Validate.isTrue(!RequestType.DeleteRequest.toString().equals(mitigationRequestDescription.getRequestType()),
                "Can not rollback to a delete request. To delete a mitigation, use delete mitigation API");
    }

    /**
     * Validates if the request object passed to the ListActiveMitigationsForService API is valid
     * @param A ListActiveMitigationsForServiceRequest object representing the input to the ListActiveMitigationsForService API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateListActiveMitigationsForServiceRequest(@NonNull ListActiveMitigationsForServiceRequest request) {
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
        validateListOfLocations(request.getLocations(), request.getServiceName());
    }
    
    private void validateDeviceAndService(String deviceName, String serviceName) {
        validateServiceName(serviceName);
        DeviceName device = validateDeviceName(deviceName);
        validateDeviceMatchService(device, serviceName);
    }
    
    /**
     * Validates if the request object passed to the ReportInactiveLocation API is valid
     * @param A ReportInactiveLocationRequest object representing the input to the ReportInactiveLocation API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateReportInactiveLocation(@NonNull ReportInactiveLocationRequest request) {
        validateDeviceAndService(request.getDeviceName(), request.getServiceName());
        validateListOfLocations(Lists.newArrayList(request.getLocation()), request.getServiceName());
    }
    
    public void validateCreateBlackholeDeviceRequest(@NonNull CreateBlackholeDeviceRequest request) {
        if (request.getBlackholeDeviceInfo() == null) {
            String msg = "No blackhole device info found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        validateBlackholeDeviceInfo(request.getBlackholeDeviceInfo());
        if (request.getBlackholeDeviceInfo().getVersion() != null) {
            String msg = "Version for a new device must be null";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    public void validateGetBlackholeDeviceRequest(@NonNull GetBlackholeDeviceRequest request) {
        validateBlackholeDeviceName(request.getName());
    }
    
    public void validateUpdateBlackholeDeviceRequest(@NonNull UpdateBlackholeDeviceRequest request) {
        if (request.getBlackholeDeviceInfo() == null) {
            String msg = "No blackhole device info found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        validateBlackholeDeviceInfo(request.getBlackholeDeviceInfo());
        if (request.getBlackholeDeviceInfo().getVersion() == null) {
            String msg = "Version for update must be not be null";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    public void validateCreateTransitProviderRequest(@NonNull CreateTransitProviderRequest request) {
        if (request.getTransitProviderInfo() == null) {
            String msg = "No transit provider info found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        validateTransitProviderInfo(request.getTransitProviderInfo());
    }
    
    public void validateGetTransitProviderRequest(@NonNull GetTransitProviderRequest request) {
        validateTransitProviderId(request.getId());
    }
    
    public void validateUpdateTransitProviderRequest(@NonNull UpdateTransitProviderRequest request) {
        if (request.getTransitProviderInfo() == null) {
            String msg = "No transit provider info found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        validateTransitProviderInfo(request.getTransitProviderInfo());
    }
    
    /**
     * Private helper method to validate the common parameters for some of the modification requests.
     * @param mitigationName Name of the mitigation passed in the request by the client.
     * @param mitigationTemplate Template corresponding to the mitigation in the request by the client.
     * @param serviceName Service corresponding to the mitigation in the request by the client.
     * @param actionMetadata Instance of MitigationActionMetadata specifying some of the metadata related to this action.
     */
    private void validateCommonModificationRequestParameters(@NonNull MitigationModificationRequest request) {
        validateMitigationName(request.getMitigationName());
        
        validateMitigationTemplate(request.getMitigationTemplate());
        
        MitigationActionMetadata actionMetadata = request.getMitigationActionMetadata();
        if (actionMetadata == null) {
            String msg = "No MitigationActionMetadata found! Passing the MitigationActionMetadata is mandatory for performing any actions using the MitigationService.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateUserName(actionMetadata.getUser());
        validateToolName(actionMetadata.getToolName());
        validateMitigationDescription(actionMetadata.getDescription());
        
        if (actionMetadata.getRelatedTickets() != null) {
            validateRelatedTickets(actionMetadata.getRelatedTickets());
        }
        
        String template = request.getMitigationTemplate();
        DeviceName device = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(template).getDeviceName();
        validateDeviceAndService(device.name(), request.getServiceName());
    }
    
    private DeviceName validateDeviceName(String deviceName) {
        if (isInvalidFreeFormText(deviceName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid device name found! Valid device names: " + deviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceName is not defined within the DeviceName enum.
        try {
            return DeviceName.valueOf(deviceName);
        } catch (Exception ex) {
            String msg = "The device name that was provided, " + deviceName + ", is not a valid name. Valid deviceNames: " + deviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateServiceName(String serviceName) {
        if (isInvalidFreeFormText(serviceName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid service name found: " + serviceName + ". Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!serviceNames.contains(serviceName)) {
            String msg = "Invalid service name found: " + serviceName + ". Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateDeviceScope(String deviceScope) {
        if (isInvalidFreeFormText(deviceScope, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid device scope found! Valid device scopes: " + deviceScopes;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceScope is not defined within the DeviceScope enum.
        try {
            DeviceScope.valueOf(deviceScope);
        } catch (Exception ex) {
            String msg = "The device scope that was provided, " + deviceScope + ", is not valid. Valid device scopes: " + deviceScopes;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateMitigationName(String mitigationName) {
        if (isInvalidFreeFormText(mitigationName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid mitigation name found! A valid mitigation name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateMitigationTemplate(String mitigationTemplate) {
        if (isInvalidFreeFormText(mitigationTemplate, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid mitigation template found! Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!mitigationTemplates.contains(mitigationTemplate)) {
            String msg = "Invalid mitigation template found: " + mitigationTemplate + ". Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private void validateBlackWatchMitigationTemplate(String mitigationTemplate) {
        validateMitigationTemplate(mitigationTemplate);
        Set<String> blackwatchMitigationTemplates = Sets.newHashSet(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer, MitigationTemplate.BlackWatchPOP_EdgeCustomer, MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer);
        if (!blackwatchMitigationTemplates.contains(mitigationTemplate)) {
            String msg = "None BlackWatch mitigation template found: " + mitigationTemplate + ". only support blackwatch mitigation template: " + blackwatchMitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateUserName(String userName) {
        if (isInvalidFreeFormText(userName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid user name found! A valid user name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    
    private static void validateMitigationId(String mitigationId) {
        if (isInvalidFreeFormText(mitigationId, false, DEFAULT_MAX_LENGTH_MITIGATION_ID)) {
            String msg = "Invalid mitigation ID! A valid mitigation ID must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_MITIGATION_ID + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateResourceId(String resourceId) {
        if (isInvalidFreeFormText(resourceId, false, DEFAULT_MAX_LENGTH_RESOURCE_ID)) {
            String msg = "Invalid resource ID! A valid resource ID must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_RESOURCE_ID + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateUserARN(String userARN) {
        if (userARN == null || 
                isInvalidFreeFormText(userARN, false, DEFAULT_MAX_LENGTH_OWNER_ARN)) {
            String msg = "Invalid user ARN found! A valid user name ARN must not be null and contain more than 0 "
                    + "and less than: " + DEFAULT_MAX_LENGTH_OWNER_ARN + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void validateResourceType(String resourceType) {
        if (isInvalidFreeFormText(resourceType, false, DEFAULT_MAX_LENGTH_RESOURCE_TYPE)) {
            String msg = "Invalid resource type found! A valid resource type must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_RESOURCE_TYPE + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        try {
            BlackWatchMitigationResourceType.valueOf(resourceType);
        }  catch (IllegalArgumentException illestEx) {
            //Catch the exception and throw a new one with a better message.
            String message = String.format("Unsupported resource type specified:%s  Acceptable values:%s", 
                    resourceType, Arrays.toString(BlackWatchMitigationResourceType.values()));
            throw new IllegalArgumentException(message);
        }
    }
    
    private static void validMaxNumberOfEntriesToFetch(long value, long min, long max) {
        Validate.isTrue((value > min) && 
                (value <= max),
                String.format("Invalid maxNumberOfEntriesToFetch found, valid number should > %d, and <= %d", min, max));
    }
    
    private static void validateToolName(String toolName) {
        if (isInvalidFreeFormText(toolName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid tool name found! A valid tool name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateMitigationDescription(String mitigationDescription) {
        if (isInvalidFreeFormText(mitigationDescription, false, DEFAULT_MAX_LENGTH_DESCRIPTION)) {
            String msg = "Invalid description found! A valid description must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_DESCRIPTION + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static void validateRelatedTickets(List<String> relatedTickets) {
        if (relatedTickets.size() > MAX_NUMBER_OF_TICKETS) {
            String msg = "Exceeded the number of tickets that can be specified for a single mitigation. Max allowed: " + MAX_NUMBER_OF_TICKETS + ", but found " + relatedTickets.size() + " tickets.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        Set<String> setOfRelatedTickets = new HashSet<String>(relatedTickets);
        if (setOfRelatedTickets.size() != relatedTickets.size()) {
            String msg = "Duplicate related tickets found in actionMetadata: " + relatedTickets;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We currently (02/2015) don't constraint the user of how they want to input the tickets, either as just ids (0043589677) or 
        // abbreviated link: tt/0043589677 or the entire link: https://tt.amazon.com/0043589677. Hence simply checking to just ensure we have all valid characters for each of those.
        for (String ticket : relatedTickets) {
            if (isInvalidFreeFormText(ticket, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
                String msg = "Invalid ticket found! A valid ticket must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    private void validateListOfLocations(List<String> locations, String serviceName) {
        if (locations != null) {
            if (locations.size() > MAX_NUMBER_OF_LOCATIONS) {
                String msg = "Exceeded the number of locations that can be specified for a single request. Max allowed: " + MAX_NUMBER_OF_LOCATIONS +
                             ". Instead found: " + locations.size() + " number of locations.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            if (CollectionUtils.isEmpty(locations)) {
                String msg = "Empty list of locations found! Either the locations should be null or must contain valid values.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            Set<String> validLocationsForService = serviceLocationsHelper.getLocationsForService(serviceName).orNull();
            List<String> invalidLocationsInRequest = new ArrayList<>();
            for (String location : locations) {
                if (isInvalidFreeFormText(location, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
                    String msg = "Invalid location name found! A valid location name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
                
                if ((validLocationsForService != null) && !validLocationsForService.contains(location)) {
                    invalidLocationsInRequest.add(location);
                }
            }
            
            if (!invalidLocationsInRequest.isEmpty()) {
                String msg = "Invalid location name found! Locations: " + invalidLocationsInRequest + " aren't valid for service: " + serviceName;
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    private void validateLocation(String location) {
        if (isInvalidFreeFormText(location, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid location name found! A valid location name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateChangeReason(String reason) {
        if (isInvalidFreeFormText(reason, false, DEFAULT_MAX_LENGTH_DESCRIPTION)) {
            String msg = "Invalid change reason found! A valid change reason must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_DESCRIPTION + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateLocationType(String locationType) {
        if (locationType != null && !locationType.equals("")) {
            if (checkLocationTypeInEnum(locationType) == false) {
                String msg = "Invalid location type found - " + locationType +". A valid location type must be one of " + Arrays.toString(LocationType.values());
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    private boolean checkLocationTypeInEnum(String locationType) {
        try {
            LocationType.valueOf(locationType);
            return true;
        } catch (IllegalArgumentException argumentException) {
            return false;
        }
    }
    
    private static final String BLACKHOLE_DEVICE_DESCRIPTION_ERROR_MSG = 
            String.format(
                    "Blackhole device description must be an non empty string of ascii-printable " + 
                    "characters shorter than %d characters", DEFAULT_MAX_LENGTH_DESCRIPTION);
    
    private static void validateBlackholeDeviceInfo(BlackholeDeviceInfo blackholeDeviceInfo) {
        validateBlackholeDeviceName(blackholeDeviceInfo.getDeviceName());
        
        String deviceDescription = blackholeDeviceInfo.getDeviceDescription();
        if (isInvalidFreeFormText(deviceDescription, false, DEFAULT_MAX_LENGTH_DESCRIPTION)) {
            LOG.info(BLACKHOLE_DEVICE_DESCRIPTION_ERROR_MSG);
            throw new IllegalArgumentException(BLACKHOLE_DEVICE_DESCRIPTION_ERROR_MSG);
        }
        
        if (blackholeDeviceInfo.getVersion() != null && blackholeDeviceInfo.getVersion() < 0) {
            String msg = "The device version that was provided, " + blackholeDeviceInfo.getVersion() + ", is not valid.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static final String TRANSIT_PROVIDER_ID_ERROR_MSG = 
            String.format("Transit provider id must be an url safe base64 encoded string.");
    
    public static void validateTransitProviderId(String transitProviderId) {
        if (StringUtils.isEmpty(transitProviderId)) {
            LOG.info(TRANSIT_PROVIDER_ID_ERROR_MSG);
            throw new IllegalArgumentException(TRANSIT_PROVIDER_ID_ERROR_MSG);
        }
        
        byte[] transitProviderBytes;
        try {
            transitProviderBytes = Base64.getUrlDecoder().decode(transitProviderId);
        } catch (IllegalArgumentException e) {
            LOG.info(TRANSIT_PROVIDER_ID_ERROR_MSG);
            throw new IllegalArgumentException(TRANSIT_PROVIDER_ID_ERROR_MSG, e);
        }
        
        if (transitProviderBytes.length != 16) {
            LOG.info(TRANSIT_PROVIDER_ID_ERROR_MSG);
            throw new IllegalArgumentException(TRANSIT_PROVIDER_ID_ERROR_MSG);
        }
    }
    
    private static void validateTransitProviderInfo(TransitProviderInfo transitProviderInfo) {
        validateTransitProviderId(transitProviderInfo.getId());
        
        validateFreeFormText("The transit provider name", 
                transitProviderInfo.getProviderName(), false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS);
        
        validateFreeFormText("The transit provider description", 
                transitProviderInfo.getProviderDescription(), false, DEFAULT_MAX_LENGTH_DESCRIPTION);
        
        if (!TransitProvider.TRANSIT_PROVIDER_STATUSES.contains(transitProviderInfo.getBlackholeSupported())) {
            String msg = "Unrecognized blackhole supported status. Allowed values for blackhole supported are "
                    + TransitProvider.TRANSIT_PROVIDER_STATUSES;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateCommunityString(transitProviderInfo.getCommunityString());
        
        if (transitProviderInfo.getBlackholeSupported().equals(TransitProvider.BLACKHOLE_SUPPORTED) &&
            StringUtils.isEmpty(transitProviderInfo.getCommunityString())) 
        {
            String msg = "The community string may not be empty for an enabled transit provider";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateFreeFormText("The transit provider blackhole link", 
                transitProviderInfo.getManualBlackholeLink(), true, DEFAULT_MAX_LENGTH_DESCRIPTION);
        
        if (transitProviderInfo.getVersion() != null && transitProviderInfo.getVersion() < 0) {
            String msg = "The transit provider version that was provided, " + transitProviderInfo.getVersion() + ", is not valid.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private static String COMMUNITY_STRING_ERROR_MSG = 
            "The community string must be a space seperated list of <asn>:<value> where asn and value are integers.";
    
    private static String COMMUNITY_STRING_ASN_ERROR_MSG = "All ASNs in a community string must match.";
    
    public static void validateCommunityString(String communityString) {
        if (StringUtils.isEmpty(communityString)) {
            // Empty community strings are allowed for disabled or manual transit providers
            return;
        }
        
        if (StringUtils.isBlank(communityString)) {
            //Empty community strings are allowed for disabled or manual transit providers, but blank one is not.
            LOG.info(COMMUNITY_STRING_ERROR_MSG);
            throw new IllegalArgumentException(COMMUNITY_STRING_ERROR_MSG);
        }
        
        if (communityString.length() > DEFAULT_MAX_LENGTH_DESCRIPTION) {
            // 100 characters should be more than enough for any sane community string
            String message = "Community string must be no more than than " + DEFAULT_MAX_LENGTH_DESCRIPTION + " characters long";
            LOG.info(message);
            throw new IllegalArgumentException(message);
        }
        
        int asn = -1;
        
        String[] values = communityString.split(" ");
        for (String value : values) {
            String parts[] = value.split(":");
            if (parts.length != 2) {
                LOG.info(COMMUNITY_STRING_ERROR_MSG);
                throw new IllegalArgumentException(COMMUNITY_STRING_ERROR_MSG);
            }
            
            try {
                int newAsn = Integer.parseInt(parts[0]);
                if (asn == -1) {
                    asn = newAsn; 
                } else if (newAsn != asn) {
                    LOG.info(COMMUNITY_STRING_ASN_ERROR_MSG);
                    throw new IllegalArgumentException(COMMUNITY_STRING_ASN_ERROR_MSG);
                }
                
                Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                LOG.info(COMMUNITY_STRING_ERROR_MSG);
                throw new IllegalArgumentException(COMMUNITY_STRING_ERROR_MSG);
            }
        }
    }
    
    private static final Pattern BLACKHOLE_DEVICE_NAME_PATTERN = Pattern.compile("[A-Za-z0-9-.]*");
            
    private static final String BLACKHOLE_DEVICE_NAME_ERROR_MSG = 
            String.format(
                    "Blackhole device name must be an non empty string shorter than %d characters containing " +
                    "only alpha numeric characters, '-' and '.'", MAX_LENGTH_BLACKHOLE_DEVICE);
    
    /**
     * Check that stringToCheck is valid for a blackhole device name. If it isn't then an IllegalArgumentException
     * will be thrown. 
     * 
     * @param deviceName the string to check
     * @throws IllegalArgumentException if the stringToCheck is invalid
     */
    private static void validateBlackholeDeviceName(String deviceName) {
        if (StringUtils.isBlank(deviceName) || deviceName.length() > MAX_LENGTH_BLACKHOLE_DEVICE ||
            !BLACKHOLE_DEVICE_NAME_PATTERN.matcher(deviceName).matches())
        {
            LOG.info(BLACKHOLE_DEVICE_NAME_ERROR_MSG);
            throw new IllegalArgumentException(BLACKHOLE_DEVICE_NAME_ERROR_MSG);
        }
    }
    
    private static boolean isInvalidFreeFormText(String stringToCheck, boolean allowBlank, int maxLength) {
        if (StringUtils.isBlank(stringToCheck)) {
            if (!allowBlank) {
                return true;
            } else {
                return false;
            }
        }
        
        if (!StringUtils.isAsciiPrintable(stringToCheck)) {
            return true;
        }
        
        if ((stringToCheck != null) && (stringToCheck.length() > maxLength)) {
            return true;
        }
        
        return false;
    }
    
    private static void validateFreeFormText(String fieldDescription, String stringToCheck, boolean allowBlank, int maxLength) {
        if (isInvalidFreeFormText(stringToCheck, allowBlank, maxLength)) {
            String message = 
                    fieldDescription + " must be a non empty string of ascii-printable characters " +
                    "with length of at most " + maxLength + " characters.";
            LOG.info(message);
            throw new IllegalArgumentException(message);
        }
    }
    
    public static void validateTemplateMatch(String mitigationNameToChange, String existingMitigationTemplate,
            String templateForMitigationToChange, String deviceName, String deviceScope) {
        
        if (!existingMitigationTemplate.equals(templateForMitigationToChange)) {
            String msg = "Found an active mitigation: " + mitigationNameToChange + " but for template: "
                    + existingMitigationTemplate + " instead of the template: " + templateForMitigationToChange
                    + " passed in the request for device: " + deviceName + " in deviceScope: " + deviceScope; 
            LOG.warn(msg);
            throw new IllegalArgumentException(msg);
        }
    }
     
    private void validateLocationOnDeviceAndService(DeviceName device, String service, String location) {
        validateLocationsOnDeviceAndService(device, service, ImmutableSet.of(location));
    }
    
    private void validateDeviceMatchService(DeviceName device, String service) {
        String errorMessage = String.format("Service %s does not match device %s", service, device);
        switch (device) {
        case POP_ROUTER:
            Validate.isTrue(ServiceName.Route53.equals(service), errorMessage);
            break;
        case POP_HOSTS_IP_TABLES:
            Validate.isTrue(ServiceName.Edge.equals(service), errorMessage);
            break;
        case ARBOR:
            Validate.isTrue(ServiceName.Blackhole.equals(service), errorMessage);
            break;
        case BLACKWATCH_POP:
            Validate.isTrue(ServiceName.Edge.equals(service), errorMessage);
            break;
        case BLACKWATCH_BORDER:
            Validate.isTrue(ServiceName.AWS.equals(service), errorMessage);
            break;
        default:
            throw new IllegalArgumentException("Unsupported device name " + device);
        }
    }
    
    private void validateLocationsOnDeviceAndService(DeviceName device, String service, Collection<String> locations) {
        String errorMessage = String.format("Not all of the locations %s in request,"
                + " are valid locations on device %s and service name %s.", locations, device, service);
    
        switch (device) {
        case POP_ROUTER:
            Validate.isTrue(edgeLocationsHelper.getAllClassicPOPs().containsAll(locations), errorMessage);
            break;
        case POP_HOSTS_IP_TABLES:
            Validate.isTrue(ImmutableSet.of("EdgeWorldwide").containsAll(locations), errorMessage);
            break;
        case ARBOR:
            Validate.isTrue(ImmutableSet.of("Arbor").containsAll(locations), errorMessage);
            break;
        case BLACKWATCH_POP:
            blackWatchEdgeLocationValidator.validateLocations(locations, errorMessage);
            break;
        case BLACKWATCH_BORDER:
            blackWatchBorderLocationValidator.validateLocations(locations, errorMessage);
            break;
        default:
            throw new IllegalArgumentException("Unsupported device name " + device);
        }
    }

    public BlackWatchTargetConfig validateUpdateBlackWatchMitigationRequest(
            @NonNull UpdateBlackWatchMitigationRequest request,
            @NonNull BlackWatchTargetConfig existingTargetConfig,
            @NonNull String userARN) {
        validateUserName(request.getMitigationActionMetadata().getUser());
        validateToolName(request.getMitigationActionMetadata().getToolName());
        if (request.getMitigationActionMetadata().getRelatedTickets() != null) {
            validateRelatedTickets(request.getMitigationActionMetadata().getRelatedTickets());
        }
        validateMitigationId(request.getMitigationId());
        validateMinutesToLive(request.getMinutesToLive());

        BlackWatchTargetConfig targetConfig = existingTargetConfig;
        boolean errorOnDuplicateRates = false;

        // If the request provided new JSON, use it instead of the existing target config
        if (request.getMitigationSettingsJSON() != null) {
            targetConfig = parseMitigationSettingsJSON(request.getMitigationSettingsJSON());
            errorOnDuplicateRates = true;
        }

        mergeGlobalPpsBps(targetConfig, request.getGlobalPPS(), request.getGlobalBPS(),
                errorOnDuplicateRates);
        validateTargetConfig(targetConfig);
        validateUserARN(userARN);

        return targetConfig;
    }  

    public BlackWatchTargetConfig validateApplyBlackWatchMitigationRequest(
            @NonNull ApplyBlackWatchMitigationRequest request, String userARN) {
        validateUserName(request.getMitigationActionMetadata().getUser());
        validateToolName(request.getMitigationActionMetadata().getToolName());
        if (request.getMitigationActionMetadata().getRelatedTickets() != null) {
            validateRelatedTickets(request.getMitigationActionMetadata().getRelatedTickets());
        }
        validateResourceId(request.getResourceId());
        validateResourceType(request.getResourceType());
        validateMinutesToLive(request.getMinutesToLive());

        // Parse the mitigation settings JSON
        BlackWatchTargetConfig targetConfig = parseMitigationSettingsJSON(request.getMitigationSettingsJSON());

        // Merge global PPS/BPS into the target config
        mergeGlobalPpsBps(targetConfig, request.getGlobalPPS(), request.getGlobalBPS());

        // Validate the new target configuration
        validateTargetConfig(targetConfig);

        validateUserARN(userARN);
        return targetConfig;
    }
    
    void mergeGlobalPpsBps(@NonNull BlackWatchTargetConfig targetConfig,
            Long globalPps, Long globalBps) {
        mergeGlobalPpsBps(targetConfig, globalPps, globalBps, true);
    }

    // Merge the GlobalPPS/GlobalBPS values provided via the API fields into the target config.
    void mergeGlobalPpsBps(@NonNull BlackWatchTargetConfig targetConfig,
            Long globalPps, Long globalBps, boolean errorOnDuplicateRates) {
        if (globalPps == null && globalBps == null) {
            return;  // There is nothing to do here
        }

        // If the mitigation_config key doesn't exist, create it
        if (targetConfig.getMitigation_config() == null) {
            targetConfig.setMitigation_config(new BlackWatchTargetConfig.MitigationConfig());
        }
        BlackWatchTargetConfig.MitigationConfig mitigationConfig = targetConfig.getMitigation_config();
        assert mitigationConfig != null;

        // If global_traffic_shaper key doesn't exist, create it
        if (mitigationConfig.getGlobal_traffic_shaper() == null) {
            mitigationConfig.setGlobal_traffic_shaper(
                    new LinkedHashMap<String, BlackWatchTargetConfig.GlobalTrafficShaper>());
        }
        Map<String, BlackWatchTargetConfig.GlobalTrafficShaper> globalShaper =
            mitigationConfig.getGlobal_traffic_shaper();
        assert globalShaper != null;

        // If the global default shaper doesn't exist, create it
        if (globalShaper.get(DEFAULT_SHAPER_NAME) == null) {
            globalShaper.put(DEFAULT_SHAPER_NAME, new BlackWatchTargetConfig.GlobalTrafficShaper());
        }
        BlackWatchTargetConfig.GlobalTrafficShaper defaultShaper = globalShaper.get(DEFAULT_SHAPER_NAME);
        assert defaultShaper != null;

        if (globalPps != null) {
            if (errorOnDuplicateRates && defaultShaper.getGlobal_pps() != null) {
                String msg = "Cannot specify global PPS rate limit using both API field and JSON";
                throw new IllegalArgumentException(msg);
            }

            defaultShaper.setGlobal_pps(globalPps);
        }

        if (globalBps != null) {
            if (errorOnDuplicateRates && defaultShaper.getGlobal_bps() != null) {
                String msg = "Cannot specify global BPS rate limit using both API field and JSON";
                throw new IllegalArgumentException(msg);
            }

            defaultShaper.setGlobal_bps(globalBps);
        }
    }

    private void validateMinutesToLive(Integer minutesToLive) {
        //Allow null and zero to be overridden.
        if (minutesToLive != null && minutesToLive != 0) {
            Validate.inclusiveBetween(1, MAX_MINUTES_TO_LIVE, minutesToLive, 
                    String.format("Minutes to live must be between 1 and %d (%d days) Invalid value:%d",
                            MAX_MINUTES_TO_LIVE, MAX_MINUTES_TO_LIVE_DAYS, minutesToLive));
        }
    }
    
    private void validatePacketsPerSecond(Long pps, String shaperName) {
        if (pps != null) {
            Validate.inclusiveBetween(0, MAX_PPS, pps, 
                    String.format("Traffic shaper \"" + shaperName + "\" must specify PPS"
                        + " (packets per second) between 0 and %d Invalid value:%d", MAX_PPS, pps));
        }
    }

    private void validateBitsPerSecond(Long bps) {
        if (bps != null) {
            Validate.inclusiveBetween(0, MAX_BPS, bps, 
                    String.format("BPS (bits per second) must be between 0 and %d Invalid value:%d",
                            MAX_BPS, bps));
        }
    }

    public static BlackWatchTargetConfig parseMitigationSettingsJSON(String mitigationSettingsJSON) {
        BlackWatchTargetConfig targetConfig;
        try {
            targetConfig = BlackWatchTargetConfig.fromJSONString(mitigationSettingsJSON);
        } catch (JsonMappingException e) {
            String msg = String.format("Could not map mitigation settings JSON to target config: %s",
                    e.getMessage());
            LOG.warn("Mitigation settings error path: " + e.getPathReference());
            LOG.warn("Mitigation settings error location: " + e.getLocation());
            LOG.warn("Mitigation settings error message: " + e.getOriginalMessage());
            throw new IllegalArgumentException(msg, e);
        } catch (JsonProcessingException e) {
            String msg = String.format("Could not parse mitigation settings JSON: %s",
                    e.getMessage());
            LOG.warn("Mitigation settings error location: " + e.getLocation());
            LOG.warn("Mitigation settings error message: " + e.getOriginalMessage());
            throw new IllegalArgumentException(msg, e);
        } catch (IOException e) {
             String msg = String.format("Could not read mitigation settings JSON: %s",
                    e.getMessage());
             throw new IllegalArgumentException(msg, e);
        }

        return targetConfig;
    }

    void validateTargetConfig(BlackWatchTargetConfig targetConfig) {
        // Don't allow specifying both ip_traffic_shaper and global_traffic_shaper
        if (targetConfig.getMitigation_config() != null
                && targetConfig.getMitigation_config().getIp_traffic_shaper() != null
                && targetConfig.getMitigation_config().getGlobal_traffic_shaper() != null) {
            throw new IllegalArgumentException("Can't configure both ip_traffic_shaper and global_traffic_shaper.");
        }

        // Validate network_acl
        if (targetConfig.getMitigation_config() != null
                && targetConfig.getMitigation_config().getNetwork_acl() != null
                && targetConfig.getMitigation_config().getNetwork_acl().getConfig() != null) {

            // non-fragment network ACL rules
            if (targetConfig.getMitigation_config().getNetwork_acl().getConfig().getRules() == null) {
                throw new IllegalArgumentException("rules key is missing in network_acl");
            }
            for (BlackWatchTargetConfig.NetworkAclMitigationConfig.ACLEntry rule :
                        targetConfig.getMitigation_config().getNetwork_acl().getConfig().getRules()) {
                if ((rule.getSrc() == null) == (rule.getSrc_country() == null)) {
                    throw new IllegalArgumentException("ACL rule should have either source ip (src)" +
                            " or source country (src_country) field, but not both");
                }
            }

            // fragment network ACL rules
            if (targetConfig.getMitigation_config().getNetwork_acl().getConfig().getFragments_rules() != null) {
                for (BlackWatchTargetConfig.NetworkAclMitigationConfig.ACLEntryForFragmented rule :
                            targetConfig.getMitigation_config().getNetwork_acl().getConfig().getFragments_rules()) {
                    if ((rule.getSrc() == null) == (rule.getSrc_country() == null)) {
                        throw new IllegalArgumentException("Fragment ACL rule should have either source ip (src)" +
                                " or source country (src_country) field, but not both");
                    }
                }
            }
        }

        // Validate global_traffic_shaper
        if (targetConfig.getMitigation_config() != null
                && targetConfig.getMitigation_config().getGlobal_traffic_shaper() != null) {
            // global_traffic_shaper key was specified
            Map<String, BlackWatchTargetConfig.GlobalTrafficShaper> globalShapers =
                targetConfig.getMitigation_config().getGlobal_traffic_shaper();

            Map<String, Long> shaperRates = new HashMap<>();

            // For each global shaper, validate that the "global_pps" key was provided.
            // Choose to do this here instead of in the model in order to eventually support
            // BPS.  With BPS support, instead validate that PPS XOR BPS is specified.
            for (Map.Entry<String, BlackWatchTargetConfig.GlobalTrafficShaper> entry : globalShapers.entrySet()) {
                String shaperName = entry.getKey();

                BlackWatchTargetConfig.GlobalTrafficShaper globalShaper = entry.getValue();
                Long ppsRate = globalShaper.getGlobal_pps();

                if (ppsRate == null) {
                    String msg = "Configured global_traffic_shaper \"" + shaperName
                        + "\" must specify \"global_pps\".";
                    throw new IllegalArgumentException(msg);
                }

                validatePacketsPerSecond(ppsRate, shaperName);

                // Error on duplicate shaper names
                if (shaperRates.containsKey(shaperName)) {
                    String msg = "Duplicate shaper name in global_traffic_shaper: \"" + shaperName + "\"";
                    throw new IllegalArgumentException(msg);
                }
                shaperRates.put(shaperName, ppsRate);
            }

            // Require a default rate to be defined somewhere
            if (!shaperRates.containsKey(DEFAULT_SHAPER_NAME)) {
                String msg = "A default rate limit must be specified";
                throw new IllegalArgumentException(msg);
            }

            // If there is more than one shaper (i.e. the default plus others),
            // don't accept a rate limit of zero for any.
            // In this case the user has specified config via JSON, and should
            // do it properly (with a DENY rule instead)
            if (shaperRates.size() > 1) {
                for (Map.Entry<String, Long> entry : shaperRates.entrySet()) {
                    if (entry.getValue().longValue() < MIN_PPS) {
                        String msg = "Rate limit for traffic shaper \"" + entry.getKey() + "\" "
                            + "must be greater than the minimum: " + MIN_PPS + " pps";
                        throw new IllegalArgumentException(msg);
                    }
                }
            }
        } else {
            // A default rate limit is required to be specified somewhere.  Since
            // we don't have any global_traffic_shaper after merging, it wasn't.
            String msg = "A default rate limit must be specified";
            throw new IllegalArgumentException(msg);
        }
    }
}

