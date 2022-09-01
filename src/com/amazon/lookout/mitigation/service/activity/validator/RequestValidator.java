package com.amazon.lookout.mitigation.service.activity.validator;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import com.amazon.blackwatch.bwircellconfig.model.Cell;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationStateRequest;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateRequest;
import com.amazon.lookout.mitigation.service.UpdateLocationStateRequest;
import com.amazon.lookout.mitigation.service.GetLocationOperationalStatusRequest;
import com.amazon.lookout.mitigation.service.activity.GetLocationDeploymentHistoryActivity;
import com.amazon.lookout.mitigation.service.activity.GetMitigationHistoryActivity;
import com.amazon.lookout.mitigation.service.activity.ListBlackWatchMitigationsActivity;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;

import lombok.NonNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.lookout.ip.IPUtils;
import com.amazon.lookout.mitigation.location.type.LocationType;
import com.amazon.lookout.model.RequestType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.base.Strings;

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
    
    private static final int DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS = 100;
    private static final int DEFAULT_MAX_LENGTH_MITIGATION_NAME = 200;
    
    private static final int DEFAULT_MAX_LENGTH_MITIGATION_ID = 100;
    private static final int DEFAULT_MAX_LENGTH_RESOURCE_ID = 256;
    private static final int DEFAULT_MAX_LENGTH_RESOURCE_TYPE = 20;
    private static final int DEFAULT_MAX_LENGTH_OWNER_ARN = 256;
    //Thirty days max
    private static final int MAX_MINUTES_TO_LIVE_DAYS = 30;
    private static final int MAX_MINUTES_TO_LIVE = MAX_MINUTES_TO_LIVE_DAYS*24*60;
    
    private static final int DEFAULT_MAX_LENGTH_DESCRIPTION = 500;
    
    private static final int MAX_NUMBER_OF_LOCATIONS = 200;
    private static final int MAX_NUMBER_OF_TICKETS = 10;
    
    private static final RecursiveToStringStyle recursiveToStringStyle = new RecursiveToStringStyle();

    private static final ImmutableSet<HostStatusEnum> requestStatusValues = ImmutableSet.of(
            HostStatusEnum.ACTIVE, HostStatusEnum.STANDBY, HostStatusEnum.DISABLED);

    private static final ImmutableSet<String> requestedStatusStrings = requestStatusValues.stream()
            .map(v -> v.name())
            .collect(ImmutableSet.toImmutableSet());

    static final Pattern REGIONAL_CELL_NAME_PATTERN = Pattern.compile(
            "bz[g]?-[a-z]{3}-c\\d+",
            Pattern.CASE_INSENSITIVE);

    private final Set<String> deviceNames;
    private final String currentRegion;
    private final BwirCellConfig bwirCellConfig;

    @ConstructorProperties({"currentRegion", "bwirCellConfig"})
    public RequestValidator(
            @NonNull String currentRegion, @NonNull BwirCellConfig bwirCellConfig) {
        this.currentRegion = currentRegion.toLowerCase();

        this.deviceNames = new HashSet<>();
        for (DeviceName deviceName : DeviceName.values()) {
            deviceNames.add(deviceName.name());
        }
        this.bwirCellConfig = bwirCellConfig;
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
        validateMetadata(request.getMitigationActionMetadata());
        validateMitigationId(request.getMitigationId());
    }

    public void validateChangeBlackWatchMitigationOwnerARNRequest(@NonNull ChangeBlackWatchMitigationOwnerARNRequest request) {
        validateMetadata(request.getMitigationActionMetadata());
        validateMitigationId(request.getMitigationId());
        validateUserARN(request.getNewOwnerARN());
        validateUserARN(request.getExpectedOwnerARN());
    }

    public void validateChangeBlackWatchMitigationStateRequest(@NonNull ChangeBlackWatchMitigationStateRequest request) {
        validateMetadata(request.getMitigationActionMetadata());
        validateMitigationId(request.getMitigationId());
        validateMitigationState(request.getExpectedState());
        validateMitigationState(request.getNewState());
    }
    
    public void validateListBlackWatchMitigationsRequest(@NonNull ListBlackWatchMitigationsRequest request) {
        validateMetadata(request.getMitigationActionMetadata());
        String mitigationId = request.getMitigationId();
        String resourceId = request.getResourceId();
        String resourceType = request.getResourceType();
        String ownerARN = request.getOwnerARN();
        if (mitigationId != null) {
            validateMitigationId(mitigationId);
        }

        BlackWatchMitigationResourceType blackWatchMitigationResourceType = null;
        if (resourceType != null) {
            blackWatchMitigationResourceType = validateResourceType(resourceType);
        }

        if (resourceId != null) {
            validateResourceId(resourceId, blackWatchMitigationResourceType);
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
    }
    
    /**
     * Validates if the request object passed to the GetMitigationInfo API is valid
     * @param An instance of GetMitigationInfoRequest representing the input to the GetMitigationInfo API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationInfoRequest(@NonNull GetMitigationInfoRequest request) {
        validateMitigationName(request.getMitigationName());
    }
    
    /**
     * Validates if the request object passed to the GetMitigationHistory API is valid
     * @param An instance of GetMitigationHistoryRequest representing the input to the GetMitigationHistory API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationHistoryRequest(
            GetMitigationHistoryRequest request) {
        validateMitigationName(request.getMitigationName());
        Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
        int maxHistoryEntryCount = GetMitigationHistoryActivity.MAX_NUMBER_OF_HISTORY_TO_FETCH;
        if (maxNumberOfHistoryEntriesToFetch != null) {
            Validate.isTrue((maxNumberOfHistoryEntriesToFetch > 0) &&
                    (maxNumberOfHistoryEntriesToFetch <= maxHistoryEntryCount),
                    String.format("maxNumberOfHistoryEntriesToFetch should be larger than 0, and <= %s", maxHistoryEntryCount));
        }
    }

    /**
     * Validates if the request object passed to the GetMitigationHistory API is valid
     * @param An instance of GetMitigationHistoryRequest representing the input to the GetMitigationHistory API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetLocationDeploymentHistoryRequest(GetLocationDeploymentHistoryRequest request) {
        validateLocation(request.getLocation());
        Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
        int maxHistoryEntryCount = GetLocationDeploymentHistoryActivity.MAX_NUMBER_OF_HISTORY_TO_FETCH;
        if (maxNumberOfHistoryEntriesToFetch != null) {
            Validate.isTrue((maxNumberOfHistoryEntriesToFetch > 0) && 
                    (maxNumberOfHistoryEntriesToFetch <= maxHistoryEntryCount),
                    String.format("maxNumberOfHistoryEntriesToFetch should be larger than 0, and <= %d", maxHistoryEntryCount));
        }
    }

    /**
     * Validate get mitigation definition request.
     * @param request : GetMitigationDefinitionRequest
     * throw IllegalArgumentException if it is invalid
     */
    public void validateGetMitigationDefinitionRequest(GetMitigationDefinitionRequest request) {
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
     * Validates if the request object passed to the GetLocationAdminInStatusRequest API is valid
     * @param An instance of GetLocationAdminInStatusRequest representing the input to the GetLocationAdminInStatus API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetLocationOperationalStatusRequest(GetLocationOperationalStatusRequest request) {
        validateLocation(request.getLocation());
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
     * Validates if the request object passed to RequestHostStatusChange API is valid
     * @param request An instance of RequestHostStatusChangeRequest representing the input to the RequestHostStatusChange API
     * @return HostStatusEnum enum value matching requestedHostStatus
     */
    public HostStatusEnum validateRequestHostStatusChangeRequest(RequestHostStatusChangeRequest request) {
        validateLocation(request.getLocation());
        validateHostName(request.getHostName());
        validateChangeReason(request.getReason());
        return validateRequestedStatus(request.getRequestedStatus());
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
            Validate.isTrue(currentRegion.equals(region.toLowerCase()),
                    "region: "+ region +" seems to be invalid, current mitigation service region is: "
                    + currentRegion);
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
        validateChangeId(request.getChangeId());
    }

    /**
     * Validates if the request object passed to the UpdateLocationStateRequest API is valid
     * @param An instance of UpdateLocationStateRequest representing the input to the UpdateLocationState API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateUpdateLocationStateRequest(UpdateLocationStateRequest request) {
        validateLocation(request.getLocation());
        validateChangeId(request.getChangeId());
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
    }
    
    /**
     * Private helper method to validate the common parameters for some of the modification requests.
     * @param mitigationName Name of the mitigation passed in the request by the client.
     * @param mitigationTemplate Template corresponding to the mitigation in the request by the client.
     */
    private void validateCommonModificationRequestParameters(@NonNull MitigationModificationRequest request) {
        validateMitigationName(request.getMitigationName());
        validateMitigationTemplate(request.getMitigationTemplate());
        validateMetadata(request.getMitigationActionMetadata());
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

    private HostStatusEnum validateRequestedStatus(String requestedStatus) {
        if (isInvalidFreeFormText(requestedStatus, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid requested status found! Valid requested status values: " + requestedStatusStrings;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }

        // This will throw an exception if the deviceName is not defined within the DeviceName enum.
        try {
            HostStatusEnum result = HostStatusEnum.valueOf(requestedStatus);
            if (requestStatusValues.contains(result)) {
                return result;
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception ex) {
            String msg = "The requested status that was provided, " + requestedStatus +
                    ", is not valid. Valid requested status values: " + requestedStatusStrings;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void validateMitigationName(String mitigationName) {
        if (isInvalidFreeFormText(mitigationName, false, DEFAULT_MAX_LENGTH_MITIGATION_NAME)) {
            String msg = "Invalid mitigation name found! A valid mitigation name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_MITIGATION_NAME + " ascii-printable characters.";
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

    private static void validateMitigationState(@NonNull String mitigationState) {
        try {
            MitigationState.State.valueOf(mitigationState);
        } catch (IllegalArgumentException ie) {
            String msg = String.format("Invalid mitigation state: %s! Valid mitigation states are %s",
                    mitigationState, Arrays.toString(MitigationState.State.values()));
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

    private static void validateResourceId(String resourceId,
            BlackWatchMitigationResourceType blackWatchMitigationResourceType) {
        validateResourceId(resourceId);

        if ((blackWatchMitigationResourceType != null)
                && (BlackWatchMitigationResourceType.IPAddressList == blackWatchMitigationResourceType)) {
            validateIpAddressListResourceId(resourceId);
        }

    }

    private static void validateIpAddressListResourceId(@NonNull String resourceId) {
        if (IPUtils.isValidCIDR(resourceId)) {
            String exceptionMessage = String.format("%s - Resource ID: %s",
                                                    "Invalid resource ID! An IP Address List resource ID cannot be a Network CIDR",
                                                    resourceId);
            LOG.info(exceptionMessage);
            throw new IllegalArgumentException(exceptionMessage);
        }
        if (StringUtils.containsWhitespace(resourceId)) {
            String exceptionMessage = String.format("%s - Resource ID: %s",
                                                    "Invalid resource ID! An IP Address List resource ID cannot contain whitespace characters",
                                                    resourceId);
            LOG.info(exceptionMessage);
            throw new IllegalArgumentException(exceptionMessage);
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

    private static BlackWatchMitigationResourceType validateResourceType(String resourceType) {
        if (isInvalidFreeFormText(resourceType, false, DEFAULT_MAX_LENGTH_RESOURCE_TYPE)) {
            String msg = "Invalid resource type found! A valid resource type must contain more than 0 and less than: "
                    + DEFAULT_MAX_LENGTH_RESOURCE_TYPE + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }

        try {
            return BlackWatchMitigationResourceType.valueOf(resourceType);
        } catch (IllegalArgumentException illestEx) {
            // Catch the exception and throw a new one with a better message.
            String message = String.format("Unsupported resource type specified:%s  Acceptable values:%s", resourceType,
                    Arrays.toString(BlackWatchMitigationResourceType.values()));
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

    private static void validateMetadata(final MitigationActionMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("MitigationActionMetadata is required");
        }

        validateUserName(metadata.getUser());
        validateToolName(metadata.getToolName());
        validateMitigationDescription(metadata.getDescription());

        if (metadata.getRelatedTickets() != null) {
            validateRelatedTickets(metadata.getRelatedTickets());
        }
    }
    
    private void validateListOfLocations(List<String> locations) {
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
            
            List<String> invalidLocationsInRequest = new ArrayList<>();
            for (String location : locations) {
                if (isInvalidFreeFormText(location, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
                    String msg = "Invalid location name found! A valid location name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
            }
            
            if (!invalidLocationsInRequest.isEmpty()) {
                String msg = "Invalid location name found! Locations: " + invalidLocationsInRequest + " aren't valid";
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

    private void validateHostName(String hostName) {
        if (isInvalidFreeFormText(hostName, false, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid host name found! A valid host name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
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

    private void validateChangeId(String changeId) {
        if (Strings.isNullOrEmpty(changeId)) {
            String msg = "Invalid changeId type found - " + changeId +".";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
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
            String templateForMitigationToChange, String deviceName) {
        
        if (!existingMitigationTemplate.equals(templateForMitigationToChange)) {
            String msg = "Found an active mitigation: " + mitigationNameToChange + " but for template: "
                    + existingMitigationTemplate + " instead of the template: " + templateForMitigationToChange
                    + " passed in the request for device: " + deviceName;
            LOG.warn(msg);
            throw new IllegalArgumentException(msg);
        }
    }
     
    public BlackWatchTargetConfig validateUpdateBlackWatchMitigationRequest(
            @NonNull UpdateBlackWatchMitigationRequest request,
            @NonNull String resourceType,
            @NonNull BlackWatchTargetConfig existingTargetConfig,
            @NonNull String userARN) {
        validateMetadata(request.getMitigationActionMetadata());
        validateMitigationId(request.getMitigationId());
        validateMinutesToLive(request.getMinutesToLive());

        BlackWatchTargetConfig targetConfig = existingTargetConfig;
        boolean errorOnDuplicateRates = false;

        // If the request provided new JSON, use it instead of the existing target config
        if (request.getMitigationSettingsJSON() != null) {
            targetConfig = parseMitigationSettingsJSON(request.getMitigationSettingsJSON());
            errorOnDuplicateRates = true;

            // validate BWiR resource types based on new config
            BlackWatchMitigationResourceType blackWatchMitigationResourceType = BlackWatchMitigationResourceType.valueOf(resourceType);
            validateBWIRResourceType(targetConfig, blackWatchMitigationResourceType);
        }

        BlackWatchTargetConfig.mergeGlobalPpsBps(targetConfig, request.getGlobalPPS(), request.getGlobalBPS(),
                                                 errorOnDuplicateRates);
        // The GLB resource type not need the default shaper because we generate it,
        // so call validate with defaultRateLimitRequired = !isGlbResource
        if (!request.isBypassConfigValidations()) {
            boolean isGlbResource = resourceType.equalsIgnoreCase("GLB");
            if (isGlbResource) {
                targetConfig.validateAsGlbControlPlaneConfig();
            } else {
                targetConfig.validateAsControlPlaneConfig();
            }
        }
        validateUserARN(userARN);

        return targetConfig;
    }

    public void validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(
            @NonNull UpdateBlackWatchMitigationRegionalCellPlacementRequest request,
            @NonNull String domain, @NonNull String realm) {

        ImmutableSet<String> bwirCellNames = bwirCellConfig.getCells(domain, realm).stream()
                .map(Cell::getName)
                .collect(ImmutableSet.toImmutableSet());

        List<String> cellNames = request.getCellNames(); // format - [bz-pdx-c1, bz-pdx-c2]
        // Validate cellNames --> all the Cell names should match REGIONAL_CELL_NAME_PATTERN
        for (String cellName : cellNames) {
            Matcher matcher = REGIONAL_CELL_NAME_PATTERN.matcher(cellName);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(String.format(
                        "Unrecognized cellName pattern found: '%s', expected pattern is: '%s'.",
                        cellName, REGIONAL_CELL_NAME_PATTERN));
            }

            if (bwirCellNames.stream().noneMatch(cellName::equalsIgnoreCase)) {
                throw new IllegalArgumentException(String.format(
                        "cellName: '%s' not found in Bwir cell config", cellName));
            }
        }
    }

    // Validate if the resource type is valid BWIR resource
    public void validateBWIRResourceType(BlackWatchTargetConfig targetConfig, BlackWatchMitigationResourceType blackWatchMitigationResourceType) {
        if (targetConfig.getGlobal_deployment() != null &&
                targetConfig.getGlobal_deployment().getPlacement_tags() != null) {

            if (targetConfig.getGlobal_deployment().getPlacement_tags().contains(BlackWatchTargetConfig.REGIONAL_PLACEMENT_TAG)) {
                if (!blackWatchMitigationResourceType.equals(BlackWatchMitigationResourceType.IPAddress) &&
                        !blackWatchMitigationResourceType.equals(BlackWatchMitigationResourceType.IPAddressList) &&
                        !blackWatchMitigationResourceType.equals(BlackWatchMitigationResourceType.ElasticIP)) {
                    String message = String.format("Unsupported resource type specified for BWiR:%s", blackWatchMitigationResourceType.name());
                    throw new IllegalArgumentException(message);
                }
            }
        }
    }

    public BlackWatchTargetConfig validateApplyBlackWatchMitigationRequest(
           @NonNull ApplyBlackWatchMitigationRequest request,
           String userARN) {
        validateMetadata(request.getMitigationActionMetadata());

        BlackWatchMitigationResourceType blackWatchMitigationResourceType = validateResourceType(request.getResourceType());
        validateResourceId(request.getResourceId(), blackWatchMitigationResourceType);
        validateMinutesToLive(request.getMinutesToLive());

        // Parse the mitigation settings JSON
        BlackWatchTargetConfig targetConfig = parseMitigationSettingsJSON(request.getMitigationSettingsJSON());

        // validate BWiR resource types
        validateBWIRResourceType(targetConfig, blackWatchMitigationResourceType);

        // Merge global PPS/BPS into the target config
        BlackWatchTargetConfig.mergeGlobalPpsBps(targetConfig, request.getGlobalPPS(), request.getGlobalBPS());

        // The GLB resource type not need the default shaper because we generate it,
        // so call validate with defaultRateLimitRequired = !isGlbResource
        if (!request.isBypassConfigValidations()) {
            boolean isGlbResource = request.getResourceType().equalsIgnoreCase("GLB");
            if (isGlbResource) {
                targetConfig.validateAsGlbControlPlaneConfig();
            } else {
                targetConfig.validateAsControlPlaneConfig();
            }
        } else {
            LOG.info("Validation bypassed for request, since bypass flag is set");
        }

        validateUserARN(userARN);
        return targetConfig;
    }

    private void validateMinutesToLive(Integer minutesToLive) {
        //Allow null and zero to be overridden.
        if (minutesToLive != null && minutesToLive != 0) {
            Validate.inclusiveBetween(1, MAX_MINUTES_TO_LIVE, minutesToLive, 
                    String.format("Minutes to live must be between 1 and %d (%d days) Invalid value:%d",
                            MAX_MINUTES_TO_LIVE, MAX_MINUTES_TO_LIVE_DAYS, minutesToLive));
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

}

