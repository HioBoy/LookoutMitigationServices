package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.ipset.SubnetBasedIpSet;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.arn.ARN;
import com.amazon.arn.ARNSyntaxException;
import com.amazon.blackwatch.helper.BlackWatchHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressResourceTypeValidator;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationRegionalCellPlacement;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationState.State;
import com.amazon.blackwatch.mitigation.state.model.MitigationStateSetting;
import com.amazon.blackwatch.mitigation.state.model.ResourceAllocationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchResourceTypeValidator;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.ApplyConfigError;
import com.amazon.lookout.mitigation.service.BuildConfigError;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationNotOwnedByRequestor400;
import com.amazon.lookout.mitigation.service.MitigationLimitByOwnerExceeded400;
import com.amazon.lookout.mitigation.service.StatusCodeSummary;
import com.amazon.lookout.mitigation.service.FailureDetails;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementResponse;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.workflow.helper.DogFishValidationHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;
import one.util.streamex.EntryStream;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class DDBBasedBlackWatchMitigationInfoHandler implements BlackWatchMitigationInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedBlackWatchMitigationInfoHandler.class);
    private static final int DEFAULT_MINUTES_TO_LIVE = 180;

    private final MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper;
    private final ResourceAllocationStateDynamoDBHelper resourceAllocationStateDynamoDBHelper;
    private final ResourceAllocationHelper resourceAllocationHelper;
    private final DogFishValidationHelper dogfishHelper;
    private final Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator> resourceTypeValidatorMap;
    private final Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeHelper> resourceTypeHelpers;
    private final int parallelScanSegments;
    private final String bamAndEc2OwnerArnPrefix;
    private final String realm;
    private final Map<String, Integer> mitigationLimitByOwner;

    private static final String DEFAULT_SHAPER_NAME = "default";

    private static final String QUERY_BLACKWATCH_MITIGATION_FAILURE = "QUERY_BLACKWATCH_MITIGATION_FAILED";

    private static final int MAX_BW_IPADDRESSES = 256;

    private static final int MAX_UPDATE_RETRIES = 3;
    private static final int UPDATE_RETRY_SLEEP_MILLIS = 100;
    private static final long REFRESH_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(10);

    // External clients (BAM) depend on the precise wording of this message,
    // avoid changing it if possible
    static final String TO_DELETE_CONDITIONAL_FAILURE_MESSAGE = String.format(
            "Could not save MitigationState, MitigationState must not be: %s",
            MitigationState.State.To_Delete.name());

    private List<MitigationState> mitigationStateList;
    private long lastUpdateTimestamp = 0;

    private List<MitigationState> updateMitigationStateList() {
        if (lastUpdateTimestamp + REFRESH_PERIOD_MILLIS < System.currentTimeMillis()) {
            LOG.debug("Updating the mitigation state list...");
            mitigationStateList = mitigationStateDynamoDBHelper.getAllMitigationStates(parallelScanSegments);
            lastUpdateTimestamp = System.currentTimeMillis();
        }
        return mitigationStateList;
    }

    private LocationMitigationStateSettings convertMitSSToLocMSS(MitigationStateSetting mitigationSettings) {
        return LocationMitigationStateSettings.builder()
                .withBPS(mitigationSettings.getBPS())
                .withPPS(mitigationSettings.getPPS())
                .withMitigationSettingsJSONChecksum(mitigationSettings.getMitigationSettingsJSONChecksum())
                .withJobStatus(mitigationSettings.getConfigDeploymentJobStatus())
                .build();
    }

    public long getMitigationsByOwner(String owner) {
        updateMitigationStateList();
        return mitigationStateList.stream()
                .filter(ms -> ms.getState().equals(State.Active.name()))
                .filter(ms -> ms.getOwnerARN().contains(owner))
                .count();
    }

    //TODO: 
    // we need change this method when we decide the strategy for nextToken
    // 1. need pass nextToken as a parameter, parse it and use the parsed result in the db scan request
    // 2. need construct a new nextToken and return it to the caller, so it knows if all items were returned or maxResults was hit.
    @Override
    public List<BlackWatchMitigationDefinition> getBlackWatchMitigations(String mitigationId, String resourceId,
            String resourceType, String ownerARN, long maxNumberOfEntriesToReturn, TSDMetrics tsdMetrics) {

        BlackWatchResourceTypeValidator typeValidator = null;
        Validate.notNull(tsdMetrics);
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler"
                + ".getBlackWatchMitigations")) {
            List<BlackWatchMitigationDefinition> listOfBlackWatchMitigations = new ArrayList<>();
            try {
                DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                if (resourceType != null && !resourceType.isEmpty()) {
                    BlackWatchMitigationResourceType blackWatchMitigationResourceType = BlackWatchMitigationResourceType.valueOf(resourceType);
                    typeValidator = resourceTypeValidatorMap.get(blackWatchMitigationResourceType);
                    scanExpression.addFilterCondition(MitigationState.RESOURCE_TYPE_KEY,
                        new Condition()
                            .withComparisonOperator(ComparisonOperator.EQ)
                            .withAttributeValueList(new AttributeValue().withS(resourceType)));
                }

                if (mitigationId != null && !mitigationId.isEmpty()) {
                    scanExpression.addFilterCondition(MitigationState.MITIGATION_ID_KEY, 
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(mitigationId)));
                }
                if (resourceId != null && !resourceId.isEmpty()) {
                    if (typeValidator != null) {
                        resourceId = typeValidator.getCanonicalStringRepresentation(resourceId);
                        scanExpression.addFilterCondition(MitigationState.RESOURCE_ID_KEY,
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(resourceId)));
                    } else {
                        final String finalResourceId = resourceId;
                        List<AttributeValue> resourceIdAttributeValueList = EntryStream.of(resourceTypeValidatorMap)
                            .map((keyValue) -> {
                                try {
                                    return new AttributeValue().withS(keyValue.getValue().getCanonicalStringRepresentation(finalResourceId));
                                } catch (IllegalArgumentException e) {
                                    return null;
                                }
                            } ).nonNull().toList();
                        scanExpression.addFilterCondition(MitigationState.RESOURCE_ID_KEY,
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.IN)
                                .withAttributeValueList(resourceIdAttributeValueList));
                    }
                }
                if (ownerARN != null && !ownerARN.isEmpty()) {
                    scanExpression.addFilterCondition(MitigationState.OWNER_ARN_KEY, 
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(ownerARN)));
                }
                
                List<MitigationState> mitigationStates = mitigationStateDynamoDBHelper
                        .getMitigationState(scanExpression, parallelScanSegments);
                
                for (MitigationState ms : mitigationStates) {
                    //Opted for a different POJO between the service layer and the DB layer.  Unfortunately it creates 
                    //this dirt.
                    MitigationActionMetadata mitigationActionMetadata =
                            BlackWatchHelper.bwMetadataToCoralMetadata(ms.getLatestMitigationActionMetadata());
                    
                    Map<String, MitigationStateSetting> locState = ObjectUtils.defaultIfNull(
                            ms.getLocationMitigationState(), new HashMap<String, MitigationStateSetting>());
                    Map<String, LocationMitigationStateSettings> locationMitigationState = new HashMap<>();

                    for (Map.Entry<String, MitigationStateSetting> entry: locState.entrySet()) {
                        LocationMitigationStateSettings locationMitigationStateSettings = null;
                        if (entry.getValue() != null) {
                            locationMitigationStateSettings = convertMitSSToLocMSS(entry.getValue());
                        }
                        locationMitigationState.put(entry.getKey(), locationMitigationStateSettings);
                    }

                    // PPS & BPS: return the values stored in JSON global_traffic_shaper,
                    // if they exist.  If not, fall back to the fields in MitigationState.
                    BlackWatchTargetConfig targetConfig = RequestValidator.parseMitigationSettingsJSON(
                            ms.getMitigationSettingsJSON());

                    Long ppsRate = null;

                    try {
                        ppsRate = targetConfig
                            .getMitigation_config()
                            .getGlobal_traffic_shaper()
                            .get(DEFAULT_SHAPER_NAME)
                            .getGlobal_pps();
                    } catch (NullPointerException npe) {
                        // Key doesn't exist, do nothing, ppsRate is still null
                    }

                    if (ppsRate == null) {
                        // Fallback to the value stored in MitigationState
                        ppsRate = ms.getPpsRate();
                    }

                    Long bpsRate = null;

                    try {
                        bpsRate = targetConfig
                            .getMitigation_config()
                            .getGlobal_traffic_shaper()
                            .get(DEFAULT_SHAPER_NAME)
                            .getGlobal_bps();
                    } catch (NullPointerException npe) {
                        // Key doesn't exist, do nothing, bpsRate is still null
                    }

                    if (bpsRate == null) {
                        // Fallback to the value stored in MitigationState
                        bpsRate = ms.getBpsRate();
                    }

                    BlackWatchMitigationDefinition mitigationDefinition = BlackWatchMitigationDefinition.builder()
                            .withMitigationId(ms.getMitigationId())
                            .withResourceId(ms.getResourceId())
                            .withResourceType(ms.getResourceType())
                            .withChangeTime(ms.getChangeTime())
                            .withOwnerARN(ms.getOwnerARN())
                            .withState(ms.getState())
                            .withGlobalPPS(ppsRate)
                            .withGlobalBPS(bpsRate)
                            .withMitigationSettingsJSON(ms.getMitigationSettingsJSON())
                            .withMitigationSettingsJSONChecksum(ms.getMitigationSettingsJSONChecksum())
                            .withMinutesToLiveAtChangeTime(ms.getMinutesToLive().intValue())
                            .withExpiryTime(ms.getChangeTime() + TimeUnit.MINUTES.toMillis(ms.getMinutesToLive()))
                            .withLatestMitigationActionMetadata(mitigationActionMetadata)
                            .withLocationMitigationState(locationMitigationState)
                            .withRecordedResources(ms.getRecordedResources())
                            .withFailureDetails(parseFailureDetails(ms))
                            .withAllowAutoMitigationOverride(ms.isAllowAutoMitigationOverride())
                            .build();
                    listOfBlackWatchMitigations.add(mitigationDefinition);
                    if (listOfBlackWatchMitigations.size() >= maxNumberOfEntriesToReturn) {
                        break;
                    }
                }
            } catch (Exception ex) {
                String msg = String.format("Caught exception when querying blackwatch mitigations with "
                        + "mitigationId: %s, resourceId: %s, resourceType: %s, ownerARN: %s",
                        mitigationId, resourceId, resourceType, ownerARN);
                LOG.error(msg, ex);
                subMetrics.addOne(QUERY_BLACKWATCH_MITIGATION_FAILURE);
                throw ex;
            }
            return listOfBlackWatchMitigations;
        }
    }

    private FailureDetails parseFailureDetails(MitigationState ms) {
        if (ms.getFailureDetails() == null) {
            return null;
        }

        FailureDetails failureDetails = new FailureDetails();
        final Map<String, StatusCodeSummary> statusCodes = new HashMap<>();
        final List<ApplyConfigError> applyConfigErrors = new ArrayList<>();
        final List<BuildConfigError> buildConfigErrors = new ArrayList<>();

        final com.amazon.blackwatch.mitigation.state.model.FailureDetails msFailureDetails = ms.getFailureDetails();
        if (msFailureDetails.getStatusDescriptions() != null) {
            failureDetails.setStatusDescriptions(msFailureDetails.getStatusDescriptions());
        }

        if (msFailureDetails.getStatusCodes() != null) {
            msFailureDetails.getStatusCodes().entrySet().forEach(entry -> {
                statusCodes.put(entry.getKey(),
                        StatusCodeSummary.builder().withHostCount(entry.getValue().getHostCount()).withLocationCount(entry.getValue().getLocationCount()).build());
            });
        }

        if (msFailureDetails.getApplyConfigErrors() != null) {
            msFailureDetails.getApplyConfigErrors().stream().forEach(error -> {
                applyConfigErrors
                        .add(ApplyConfigError.builder().withCode(error.getCode()).withMessage(error.getMessage()).build());
            });
        }

        if (msFailureDetails.getBuildConfigErrors() != null) {
            msFailureDetails.getBuildConfigErrors().stream().forEach(error -> {
                buildConfigErrors.add(BuildConfigError.builder().withCode(error.getCode()).withMessage(error.getMessage()).build());
            });
        }

        failureDetails.setStatusCodes(statusCodes);
        failureDetails.setApplyConfigErrors(applyConfigErrors);
        failureDetails.setBuildConfigErrors(buildConfigErrors);
        return failureDetails;
    }

    @Override
    public MitigationState getMitigationState(final String mitigationId) {
        return mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
    }

    @Override
    public UpdateBlackWatchMitigationRegionalCellPlacementResponse
    updateBlackWatchMitigationRegionalCellPlacement(final String mitigationId, final List<String> cellNames,
                                                    final String userARN, final TSDMetrics tsdMetrics) {
        // Add cell names to new cellPlacement field in Mitigation State DDB table.

        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler"
                + ".updateBlackWatchMitigationRegionalCellPlacement")) {

            MitigationState mitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);

            if (mitigationState == null) {
                String message = String.format("MitigationId:%s could not be found in MitigationState table.",
                        mitigationId);
                subMetrics.addOne("BadMitigationId");
                throw new IllegalArgumentException(message);
            }

            subMetrics.addZero("BadMitigationId");

            String mitState = mitigationState.getState();
            if(!mitState.equals(State.Active.name())) {
                throw new IllegalStateException(String.format("UpdateCellPlacement API called on an existing " +
                        "mitigation: %s which is in state: %s. " +
                        "Rather, 'Active' state is required.", mitigationId, mitState));
            }

            BlackWatchTargetConfig existingTargetConfig = null;
            try {
                existingTargetConfig = BlackWatchTargetConfig.fromJSONString(
                        mitigationState.getMitigationSettingsJSON());
            } catch (IOException e) {
                throw new IllegalStateException(String.format("Failed to parse mitigation config for " +
                        "existing mitigation %s", mitigationId), e);
            }

            if (!((existingTargetConfig != null) && (existingTargetConfig.getGlobal_deployment() != null)
                    && (existingTargetConfig.getGlobal_deployment().getPlacement_tags() != null) &&
                    (existingTargetConfig.getGlobal_deployment().getPlacement_tags()
                    .contains(BlackWatchTargetConfig.REGIONAL_PLACEMENT_TAG)))) {
                String message = String.format("Mitigation with MitigationId:%s is not a regional mitigation",
                        mitigationId);
                throw new IllegalArgumentException(message);
            }

            if (!mitigationState.getOwnerARN().equals(userARN)) {
                String message = String.format("Cannot update cell placement for mitigationId:%s as the " +
                                "calling owner:%s does not match the recorded owner:%s", mitigationId,
                        userARN, mitigationState.getOwnerARN());
                throw new MitigationNotOwnedByRequestor400(message);
            }

            mitigationState.setChangeTime(System.currentTimeMillis());

            BlackWatchMitigationRegionalCellPlacement placement = new BlackWatchMitigationRegionalCellPlacement();

            placement.setCellNames(cellNames);
            mitigationState.setRegionalPlacement(placement);
            try {
                saveMitigationState(mitigationState, false, subMetrics);

                subMetrics.addCount("ExistingMitgationModified", 1);
                UpdateBlackWatchMitigationRegionalCellPlacementResponse response =
                        new UpdateBlackWatchMitigationRegionalCellPlacementResponse();

                response.setMitigationId(mitigationId);
                return response;
            } catch (ConditionalCheckFailedException e) {
                String message = String.format("Failed to update MitigationState due to " +
                        "ConditionalCheckFailedException on DDB table");
                LOG.warn(message);
                throw e;
            }
        }
    }

    @Override
    public UpdateBlackWatchMitigationResponse updateBlackWatchMitigation(String mitigationId,
            Integer minsToLive, MitigationActionMetadata metadata, BlackWatchTargetConfig targetConfig,
            String userARN, TSDMetrics tsdMetrics, boolean bypassConfigValidations) {
        Validate.notNull(mitigationId);
        Validate.notNull(userARN);
        Validate.notNull(tsdMetrics);
        validateBypassConfigValidation(userARN, bypassConfigValidations);

        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler"
                + ".updateBlackWatchMitigation")) {
            // since Optimistic locking is enabled for MitigationState table, let's retry couple of
            // times to update the mitigation state since workers can update this in parallel
            for (int attempt = 0; attempt < MAX_UPDATE_RETRIES; attempt++) {
                MitigationState mitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);

                if (mitigationState == null) {
                    String message = String.format("MitigationId:%s could not be found in MitigationState table.", 
                            mitigationId);
                    subMetrics.addOne("BadMitigationId");
                    throw new IllegalArgumentException(message);
                } else if (mitigationState.getState().equals(MitigationState.State.To_Delete.name())) {
                    throw new IllegalArgumentException(TO_DELETE_CONDITIONAL_FAILURE_MESSAGE);
                }

                subMetrics.addZero("BadMitigationId");
                String resourceId = mitigationState.getResourceId();
                String resourceTypeString = mitigationState.getResourceType();
                BlackWatchMitigationResourceType resourceType = BlackWatchMitigationResourceType.valueOf(resourceTypeString);
                BlackWatchResourceTypeValidator typeValidator = resourceTypeValidatorMap.get(resourceType);
                if (typeValidator == null) {
                    String msg = String.format("Resource type specific validator could not be found! Type:%s", 
                            resourceTypeString);
                    throw new IllegalArgumentException(msg);
                }

                String mitigationSettingsJSON;
                if (targetConfig != null) {
                    // need to validate updated mitigation settings
                    mitigationSettingsJSON = targetConfig.getJsonString();

                    BlackWatchTargetConfig existingTargetConfig = null;
                    try {
                        existingTargetConfig = BlackWatchTargetConfig.fromJSONString(mitigationState.getMitigationSettingsJSON());
                    } catch (IOException e) {
                        throw new IllegalStateException(String.format("Failed to parse mitigation config for existing mitigation %s", mitigationId), e);
                    }

                    if (mitigationState.getState().equals(State.Failed.name())) {
                        if (existingTargetConfig.equals(targetConfig)) {
                            String message = String.format("Trying to update mitigation %s in FAILED state with same target config", mitigationState.getMitigationId());
                            throw new IllegalArgumentException(message);
                        }

                        // reset num failures when transitioning from Failed -> Active state
                        resetNumFailures(mitigationState);
                    }

                    String canonicalResourceId = typeValidator.getCanonicalStringRepresentation(resourceId);
                    Map<BlackWatchMitigationResourceType, Set<String>> resourceMap =
                            typeValidator.getCanonicalMapOfResources(resourceId, targetConfig);
                    LOG.info(String.format("Extracted canonical resource:%s and resource sets:%s",
                            canonicalResourceId, ReflectionToStringBuilder.toString(resourceMap)));
                    validateResources(resourceMap);
                } else {
                    // mitigation settings are not being updated
                    mitigationSettingsJSON = null;
                }

                String previousOwnerARN = mitigationState.getOwnerARN();
                mitigationState.setState(MitigationState.State.Active.name());
                mitigationState.setChangeTime(System.currentTimeMillis());
                mitigationState.setBypassConfigValidations(bypassConfigValidations);
                mitigationState.setOwnerARN(userARN);
                if (mitigationSettingsJSON != null) {
                    // update mitigation settings JSON
                    mitigationState.setMitigationSettingsJSON(mitigationSettingsJSON);
                    mitigationState.setMitigationSettingsJSONChecksum(
                            BlackWatchHelper.getHexStringChecksum(mitigationSettingsJSON));
                } else {
                    // do not update mitigation settings JSON
                    mitigationState.setMitigationSettingsJSON(null);
                    mitigationState.setMitigationSettingsJSONChecksum(null);
                }
                mitigationState.setMinutesToLive(minsToLive);
                BlackWatchMitigationActionMetadata bwMetadata = BlackWatchHelper.coralMetadataToBWMetadata(metadata);
                mitigationState.setLatestMitigationActionMetadata(bwMetadata);

                try {
                    saveMitigationState(mitigationState, false, subMetrics);

                    subMetrics.addCount("ExistingMitgationModified", 1);
                    UpdateBlackWatchMitigationResponse response = new UpdateBlackWatchMitigationResponse();
                    response.setMitigationId(mitigationId);
                    response.setPreviousOwnerARN(previousOwnerARN);
                    return response;
                } catch (ConditionalCheckFailedException e) {
                    try {
                        LOG.warn(String.format("ConditionalCheckFailedException while doing update "
                                + "mitigation on mitigation ID: %s, retry attempt: %d", mitigationId, attempt + 1));
                        Thread.sleep((long) ((attempt + 1)*UPDATE_RETRY_SLEEP_MILLIS*Math.random()));
                    } catch (InterruptedException intEx) {
                        // If we were interrupted then stop trying and fail immediately.
                        LOG.info("Interrupted while sleeping to retry");
                        Thread.currentThread().interrupt();
                    }
                }
            }
            String message = String.format("Failed to update MitigationState due to ConditionalCheckFailedException " +
                    "even after retrying for %d times, please try calling this API again", MAX_UPDATE_RETRIES);
            throw new IllegalArgumentException(message);
        }
    }
    
    /**
     * Validate the resource Sets
     * @param resourceMap Map of ResourceType to Set of resources.
     */
    void validateResources(Map<BlackWatchMitigationResourceType, Set<String>> resourceMap) {
        //Validate IPAddresses.
        Set<String> ipAddresses = resourceMap.getOrDefault(BlackWatchMitigationResourceType.IPAddress, 
                new HashSet<String>());
        Validate.inclusiveBetween(0, MAX_BW_IPADDRESSES, ipAddresses.size());
        ipAddresses.forEach(f -> dogfishHelper.validateCIDRInRegion(f));

        //Validate ARNs
        Set<String> arnValues = resourceMap.getOrDefault(BlackWatchMitigationResourceType.ElasticIP, new HashSet<>());
        arnValues.forEach(f -> {
            try {
                ARN.fromString(f);
            } catch (ARNSyntaxException e) {
                String msg = String.format("Resource %s is not a valid ARN, Exception: %s", f, e);
                LOG.warn(msg);
                throw new IllegalArgumentException(msg);
            }
        });
    }

    @Override
    public ApplyBlackWatchMitigationResponse applyBlackWatchMitigation(String resourceId, String resourceTypeString,
            Integer minsToLive, MitigationActionMetadata metadata, BlackWatchTargetConfig targetConfig,
            String userARN, TSDMetrics tsdMetrics, boolean allowAutoMitigationOverride, boolean bypassConfigValidations) {
        Validate.notNull(resourceId);
        Validate.notNull(resourceTypeString);
        Validate.notNull(targetConfig);
        Validate.notNull(userARN);
        Validate.notNull(tsdMetrics);
        validateBypassConfigValidation(userARN, bypassConfigValidations);
        validateUserMitigationLimitExceed(userARN);

        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler"
                + ".applyBlackWatchMitigation")) {
            boolean newMitigationCreated = false;
            String mitigationId;
            
            BlackWatchMitigationResourceType resourceType = BlackWatchMitigationResourceType.valueOf(resourceTypeString);
            BlackWatchResourceTypeValidator typeValidator = resourceTypeValidatorMap.get(resourceType);
            if (typeValidator == null) {
                String msg = String.format("Resource type specific validator could not be found! Type:%s", 
                        resourceTypeString);
                LOG.error(msg);
                throw new IllegalArgumentException(msg);
            }

            // Supporting 'ARN, IP Address' format for resources
            String ipAddress = null;
            if (resourceType.equals(BlackWatchMitigationResourceType.ElasticIP)) {
                String[] resourceData = resourceId.split(",");
                if (resourceData.length != 2) {
                    throw new IllegalArgumentException("Resource ID must be 'ARN,EIP' format for now!");
                }
                resourceId = resourceData[0];
                ipAddress = resourceData[1];
            } else if (resourceType.equals(BlackWatchMitigationResourceType.GLB)) {
                // TODO: remove the SFO boolean
                if (!realm.equalsIgnoreCase("us-west-2") && !realm.equalsIgnoreCase("us-east-1") && !realm.equalsIgnoreCase("us-west-1")) {
                    throw new IllegalArgumentException("GLB Mitigations can only be placed in PDX (us-west-2) or IAD (us-east-1).");   
                }
            } else if (resourceType.equals(BlackWatchMitigationResourceType.IPAddress)) {
                ipAddress = resourceId;
            }

            String canonicalResourceId = typeValidator.getCanonicalStringRepresentation(resourceId);
            String mitigationSettingsJSON = targetConfig.getJsonString();
            Map<BlackWatchMitigationResourceType, Set<String>> resourceMap = 
                    typeValidator.getCanonicalMapOfResources(resourceId, targetConfig);
            LOG.info(String.format("Extracted canonical resource:%s and resource sets:%s",
                    canonicalResourceId, ReflectionToStringBuilder.toString(resourceMap)));
            validateResources(resourceMap);
                        
            ResourceAllocationState resourceState = resourceAllocationStateDynamoDBHelper
                    .getResourceAllocationState(canonicalResourceId);

            // since Optimistic locking is enabled for MitigationState table, let's retry couple of
            // times to update the mitigation state since workers can update this in parallel
            for (int attempt = 0; attempt < MAX_UPDATE_RETRIES; attempt++) {
                MitigationState mitigationState;

                if (resourceState != null) {
                    Validate.isTrue(resourceState.getResourceType().equals(resourceTypeString),
                            String.format("Recorded resourceId:%s with type:%s does not match the specified type:%s",
                                    canonicalResourceId, resourceState.getResourceType(), resourceTypeString));
                    newMitigationCreated = false;
                    mitigationId = resourceState.getMitigationId();
                    mitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
                    if (mitigationState == null) {
                        String message = String.format("MitigationId:%s returned from the resource does not exist!", 
                                mitigationId);
                        throw new IllegalArgumentException(message);
                    } else if (mitigationState.getState().equals(MitigationState.State.To_Delete.name())) {
                        throw new IllegalArgumentException(TO_DELETE_CONDITIONAL_FAILURE_MESSAGE);
                    } else if (mitigationState.getState().equals(MitigationState.State.Failed.name())) {
                        BlackWatchTargetConfig existingTargetConfig = null;
                        try {
                            existingTargetConfig = BlackWatchTargetConfig.fromJSONString(mitigationState.getMitigationSettingsJSON());
                        } catch (IOException e) {
                            throw new IllegalStateException(String.format("Failed to parse mitigation config for existing mitigation %s", mitigationId), e);
                        }
                        if (existingTargetConfig.equals(targetConfig)) {
                            String message = String.format("Mitigation %s is in FAILED state, cannot apply same target config", mitigationState.getMitigationId());
                            throw new IllegalArgumentException(message);
                        }

                        // set numFailures to be 0 when transitioning to ACTIVE state
                        resetNumFailures(mitigationState);
                    }
                    if (!mitigationState.getOwnerARN().equals(userARN)) {
                        String message = String.format("Cannot apply update to mitigationId:%s as the calling owner:%s "
                                + "does not match the recorded owner:%s", mitigationId, userARN, mitigationState.getOwnerARN());
                        throw new MitigationNotOwnedByRequestor400(message);
                    }
                } else {
                    // check if ElasticIP is already allocated
                    if (resourceTypeString.equals(BlackWatchMitigationResourceType.ElasticIP.name())) {
                        Validate.notNull(ipAddress);
                        ResourceAllocationState resourceAllocationState = resourceAllocationStateDynamoDBHelper.getResourceAllocationState(ipAddress);
                        // resourceAllocationState is non-null when ElasticIP is already allocated
                        if (resourceAllocationState != null) {
                            String msg = String.format("Could not create EIP mitigation for resourceId:%s resourceType:%s "
                                    + "since ElasticIP:%s conflicts with an existing mitigation with mitigationId:%s.",
                                    canonicalResourceId, resourceType, ipAddress, resourceAllocationState.getMitigationId());
                            throw new MitigationNotOwnedByRequestor400(msg);
                        }
                    }
                    newMitigationCreated = true;
                    mitigationId = generateMitigationId(realm);
                    mitigationState = MitigationState.builder()
                            .mitigationId(mitigationId)
                            .resourceId(canonicalResourceId)
                            .allowAutoMitigationOverride(allowAutoMitigationOverride)
                            .bypassConfigValidations(bypassConfigValidations)
                            .resourceType(resourceTypeString)
                            .ownerARN(userARN)
                            .build();
                }

                subMetrics.addCount("RequestCoveredByExistingMitigation", 0);

                // Need to prevent auto-mitigations from BAM and EC2 from creating more specific
                // mitigations within the IP space already covered by existing mitigations.
                // Route53 creates and maintains long-lasting mitigations on larger IP prefixes (/22).
                if (userARN.startsWith(bamAndEc2OwnerArnPrefix) && ipAddress != null) {
                    final String ipAddressToCheck = ipAddress;

                    updateMitigationStateList();

                    Optional<MitigationState> mitigationStateWithSupersetPrefix = mitigationStateList.stream()
                            .filter(ms -> ms.getState().equals(State.Active.name()))
                            .filter(ms -> !ms.getOwnerARN().equals(userARN))
                            .filter(ms -> isRequestIpCoveredByExistingMitigation(ipAddressToCheck, ms))
                            .findAny();

                    //If customers is ok to allow BAM overriding mitigation, then do nothing.
                    //Otherwise, throw out the error
                    if (mitigationStateWithSupersetPrefix.isPresent() && !mitigationStateWithSupersetPrefix.get().isAllowAutoMitigationOverride()) {
                        String errorMsg = String.format("The request is rejected since the user %s is "
                                + "auto mitigation (BAM or EC2), and mitigation %s already exists on a superset prefix",
                                userARN,
                                mitigationStateWithSupersetPrefix.get().getMitigationId());
                        LOG.warn(errorMsg);
                        subMetrics.addCount("RequestCoveredByExistingMitigation", 1);
                        throw new MitigationNotOwnedByRequestor400(errorMsg);
                    }
                }

                mitigationState.setState(MitigationState.State.Active.name());
                mitigationState.setChangeTime(System.currentTimeMillis());
                mitigationState.setMitigationSettingsJSON(mitigationSettingsJSON);
                mitigationState.setMitigationSettingsJSONChecksum(
                        BlackWatchHelper.getHexStringChecksum(mitigationSettingsJSON));
                mitigationState.setMinutesToLive(minsToLive);
                BlackWatchMitigationActionMetadata bwMetadata = BlackWatchHelper.coralMetadataToBWMetadata(metadata);
                mitigationState.setLatestMitigationActionMetadata(bwMetadata);

                BlackWatchResourceTypeHelper resourceTypeHelper = resourceTypeHelpers.get(resourceType);
                if (resourceTypeHelper == null) {
                    String message = String.format("Resource type specific helper could not be found! Type:%s",  resourceType);
                    LOG.error(message);
                    throw new IllegalArgumentException(message);
                }
                resourceTypeHelper.updateResourceBriefInformation(mitigationState);
                LOG.debug("mitigation state after update: " + mitigationState.toString());

                try {
                    saveMitigationState(mitigationState, newMitigationCreated, subMetrics);

                    if (newMitigationCreated) {
                        boolean allocationProposalSuccess = resourceAllocationHelper.proposeNewMitigationResourceAllocation(
                                mitigationId, canonicalResourceId, resourceTypeString);
                        if (!allocationProposalSuccess) {
                            String msg = String.format("Could not complete resource allocation for mitigationId:%s "
                                    + "resourceId:%s resourceType:%s", mitigationId, canonicalResourceId, resourceType);
                            mitigationStateDynamoDBHelper.deleteMitigationState(mitigationState);
                            throw new IllegalArgumentException(msg);
                        }
                        if (resourceTypeString.equals(BlackWatchMitigationResourceType.ElasticIP.name())) {
                            Validate.notNull(ipAddress);
                            Map<String, Set<String>> addMap = ImmutableMap.of(BlackWatchMitigationResourceType.IPAddress.name(),
                                    Collections.singleton(IPAddressResourceTypeValidator.convertIPToCanonicalStringRepresentation
                                            (ipAddress)));
                            LOG.info("Allocating Resource:" + addMap.toString() + " to mitigationId:" + mitigationId);
                            resourceAllocationHelper.proposeAdditionalResourcesForMitigation(mitigationId, addMap);
                        }
                    }

                    subMetrics.addCount("NewMitgationCreated", newMitigationCreated ? 1 : 0);
                    subMetrics.addCount("ExistingMitgationModified", newMitigationCreated ? 0 : 1);

                    ApplyBlackWatchMitigationResponse response = new ApplyBlackWatchMitigationResponse();
                    response.setNewMitigationCreated(newMitigationCreated);
                    response.setMitigationId(mitigationId);
                    return response;
                } catch (ConditionalCheckFailedException e) {
                    // do not retry if its a new mitigation
                    if (newMitigationCreated) {
                        String message = "Could not save MitigationState due to conditional failure! "
                                + "Conflicting MitigationId on a new mitigation.";
                        throw new IllegalArgumentException(message);
                    }
                    try {
                        LOG.warn(String.format("ConditionalCheckFailedException while doing update "
                                + "mitigation on mitigation ID: %s, retry attempt: %d", mitigationId, attempt + 1));
                        Thread.sleep((long) ((attempt + 1)*UPDATE_RETRY_SLEEP_MILLIS*Math.random()));
                    } catch (InterruptedException intEx) {
                        // If we were interrupted then stop trying and fail immediately.
                        LOG.info("Interrupted while sleeping to retry");
                        Thread.currentThread().interrupt();
                    }
                }
            }
            String message = String.format("Failed to update MitigationState due to ConditionalCheckFailedException " +
                    "even after retrying for %d times, please try calling this API again", MAX_UPDATE_RETRIES);
            throw new IllegalArgumentException(message);
        }
    }

    private void validateUserMitigationLimitExceed(String userArn) {
        mitigationLimitByOwner.entrySet().stream().forEach((userKey) -> {
                    if (userArn.contains(userKey.getKey())) {
                        long mitigationCount = getMitigationsByOwner(userKey.getKey());
                        if (mitigationCount >= userKey.getValue()) {
                            throw new MitigationLimitByOwnerExceeded400(String.format("Owner: %s (ARN:%s) has Exceeded "
                                    + "permitted limit: %d Existing Active Mitigation Count: %d",
                                    userKey.getKey(), userArn, userKey.getValue(), mitigationCount));
                        }
                    }
                });
    }

    private void validateBypassConfigValidation(String userArn, boolean bypassConfigValidations) {
        if (bypassConfigValidations && userArn.startsWith(bamAndEc2OwnerArnPrefix)) {
            throw new IllegalArgumentException("Auto mitigations should not skip validation!");
        }
    }

    private void resetNumFailures(MitigationState mitigationState) {
        if (mitigationState.getLocationMitigationState() != null) {
            mitigationState.getLocationMitigationState().entrySet().stream().forEach((entry) -> {
                if (entry.getValue() != null) {
                    entry.getValue().setNumFailures(0);
                }
            });
        }
    }

    boolean isRequestIpCoveredByExistingMitigation(
            final String ipAddress,
            final MitigationState mitigationState) {
        return ObjectUtils.defaultIfNull(mitigationState.getRecordedResources().get(BlackWatchMitigationResourceType.IPAddress.name()),
                new HashSet<String>())
                .stream()
                .anyMatch(ip -> {
                    SubnetBasedIpSet subnetBasedIpSet = new SubnetBasedIpSet(ImmutableList.of(ip));
                    return subnetBasedIpSet.isMember(ipAddress);
                });
    }

    private void saveMitigationState(MitigationState mitigationState, boolean newMitigationCreated,
            TSDMetrics subMetrics) {
        
        Integer minsToLive = mitigationState.getMinutesToLive();
        if (minsToLive == null || minsToLive == 0) {
            mitigationState.setMinutesToLive(DEFAULT_MINUTES_TO_LIVE);
        }
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expectedAttributes = new HashMap<String, ExpectedAttributeValue>();
        if (newMitigationCreated) {
            ExpectedAttributeValue doesNotExistValue = new ExpectedAttributeValue(false);
            expectedAttributes.put(MitigationState.MITIGATION_ID_KEY, doesNotExistValue);
        } else {
            //Don't allow updates on mitigations that are being deleted.
            ExpectedAttributeValue expectedAttribute = new ExpectedAttributeValue(new AttributeValue(
                    MitigationState.State.To_Delete.name()));
            expectedAttribute.setComparisonOperator(ComparisonOperator.NE);
            expectedAttributes.put(MitigationState.STATE_KEY, expectedAttribute);
        }
        saveExpression.setExpected(expectedAttributes);

        mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(mitigationState, saveExpression);
    }

    private void failDeactivateMitigation(final String mitigationId, final int attempts) {
        final String msg = String.format("Failed to deactivate mitigationId=%s after %d retries.",
                mitigationId, attempts);
        throw new RuntimeException(msg);
    }

    public void deactivateMitigation(final String mitigationId, final MitigationActionMetadata actionMetadata) {
        final BlackWatchMitigationActionMetadata actionMetadataBlackWatch =
                BlackWatchHelper.coralMetadataToBWMetadata(actionMetadata);

        // Allow a few retries to avoid clashing with the workers
        for (int attempt = 0; attempt < MAX_UPDATE_RETRIES; attempt++) {
            MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);

            if (state == null) {
                throw new IllegalArgumentException("Specified mitigation Id " + mitigationId + " does not exist");
            } else if (state.getState().equals(MitigationState.State.Expired.name())
                    || state.getState().equals(MitigationState.State.To_Delete.name())) {
                return;  // Already in the desired state
            }

            state.setState(MitigationState.State.Expired.name());
            state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);

            // Make update conditional on the mitigation not being in To_Delete state
            DynamoDBSaveExpression expression = new DynamoDBSaveExpression();
            ExpectedAttributeValue expectNotToDelete = new ExpectedAttributeValue(
                    new AttributeValue(MitigationState.State.To_Delete.name()));
            expectNotToDelete.setComparisonOperator(ComparisonOperator.NE);
            Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
            expected.put(MitigationState.STATE_KEY, expectNotToDelete);
            expression.setExpected(expected);

            try {
                mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(
                        state, expression);
                return;
            } catch (ConditionalCheckFailedException e) {
                try {
                    Thread.sleep((long) ((attempt + 1)*UPDATE_RETRY_SLEEP_MILLIS*Math.random()));
                } catch (InterruptedException intEx) {
                    // If we were interrupted then stop trying and fail immediately.
                    LOG.info("Interrupted while sleeping to retry");
                    Thread.currentThread().interrupt();
                    failDeactivateMitigation(mitigationId, attempt);
                }
            }
        }

        // If we used all attempts and still failed to update the state, something
        // is probably wrong.
        failDeactivateMitigation(mitigationId, MAX_UPDATE_RETRIES);
    }

    public void changeOwnerARN(String mitigationId, String newOwnerARN, String expectedOwnerARN,
                            MitigationActionMetadata actionMetadata) {
        // Get current state
        MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        if (state == null) {
            throw new IllegalArgumentException("Specified mitigation Id " + mitigationId + " does not exist");
        }

        state.setOwnerARN(newOwnerARN);
        BlackWatchMitigationActionMetadata actionMetadataBlackWatch =
                BlackWatchHelper.coralMetadataToBWMetadata(actionMetadata);
        state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);
        DynamoDBSaveExpression condition = new DynamoDBSaveExpression();
        ExpectedAttributeValue expectedValue = new ExpectedAttributeValue(new AttributeValue(expectedOwnerARN));
        expectedValue.setComparisonOperator(ComparisonOperator.EQ);
        Map<String, ExpectedAttributeValue> expectedAttributes =
                ImmutableMap.of(MitigationState.OWNER_ARN_KEY, expectedValue);
        condition.setExpected(expectedAttributes);
        mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(state, condition);
    }

    public void changeMitigationState(String mitigationId, MitigationState.State expectedState, MitigationState.State newState,
                                      MitigationActionMetadata actionMetadata) {
        // Get current state
        MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        if (state == null) {
            throw new IllegalArgumentException("Specified mitigationId : " + mitigationId + " does not exist");
        }

        // reset num failures when transitioning from failed to active
        if (expectedState.equals(State.Failed) && newState.equals(State.Active)) {
            resetNumFailures(state);
        }

        state.setState(newState.name());
        BlackWatchMitigationActionMetadata actionMetadataBlackWatch =
                BlackWatchHelper.coralMetadataToBWMetadata(actionMetadata);
        state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);
        state.setChangeTime(System.currentTimeMillis());
        DynamoDBSaveExpression condition = new DynamoDBSaveExpression();
        ExpectedAttributeValue expectedValue = new ExpectedAttributeValue(new AttributeValue(expectedState.name()));
        expectedValue.setComparisonOperator(ComparisonOperator.EQ);
        Map<String, ExpectedAttributeValue> expectedAttributes =
                ImmutableMap.of(MitigationState.STATE_KEY, expectedValue);
        condition.setExpected(expectedAttributes);
        mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(state, condition);
    }
}

