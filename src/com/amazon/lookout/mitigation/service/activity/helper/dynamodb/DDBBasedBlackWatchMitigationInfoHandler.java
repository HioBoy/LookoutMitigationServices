package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.helper.BlackWatchHelper;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationStateSetting;
import com.amazon.blackwatch.mitigation.state.model.ResourceAllocationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.lookout.mitigation.blackwatch.model.BlackWatchMitigationResourceType;
import com.amazon.lookout.mitigation.blackwatch.model.BlackWatchResourceTypeValidator;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
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
import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;
import one.util.streamex.EntryStream;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    private final int parallelScanSegments;
    private final String realm;

    private static final String DEFAULT_SHAPER_NAME = "default";

    private static final String QUERY_BLACKWATCH_MITIGATION_FAILURE = "QUERY_BLACKWATCH_MITIGATION_FAILED";

    private static final int MAX_BW_IPADDRESSES = 256;

    private static final int MAX_UPDATE_RETRIES = 3;
    private static final int UPDATE_RETRY_SLEEP_MILLIS = 100;
    
    private LocationMitigationStateSettings convertMitSSToLocMSS(MitigationStateSetting mitigationSettings) {
        return LocationMitigationStateSettings.builder()
                .withBPS(mitigationSettings.getBPS())
                .withPPS(mitigationSettings.getPPS())
                .withMitigationSettingsJSONChecksum(mitigationSettings.getMitigationSettingsJSONChecksum())
                .withJobStatus(mitigationSettings.getConfigDeploymentJobStatus())
                .build();
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
                    Map<String, LocationMitigationStateSettings> locationMitigationState = EntryStream.of(locState)
                        .mapValues(v -> v != null ? convertMitSSToLocMSS(v) : null).toMap();

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

    @Override
    public MitigationState getMitigationState(final String mitigationId) {
        return mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
    }

    @Override
    public UpdateBlackWatchMitigationResponse updateBlackWatchMitigation(String mitigationId,
            Integer minsToLive, MitigationActionMetadata metadata, BlackWatchTargetConfig targetConfig,
            String userARN, TSDMetrics tsdMetrics) {
        Validate.notNull(mitigationId);
        Validate.notNull(userARN);
        Validate.notNull(tsdMetrics);
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler"
                + ".updateBlackWatchMitigation")) {
            
            MitigationState mitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
            if (mitigationState == null) {
                String message = String.format("MitigationId:%s could not be found in MitigationState table.", 
                        mitigationId);
                subMetrics.addOne("BadMitigationId");
                throw new IllegalArgumentException(message);
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
            mitigationState.setChangeTime(System.currentTimeMillis());
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
            saveMitigationState(mitigationState, false, subMetrics);
            
            UpdateBlackWatchMitigationResponse response = new UpdateBlackWatchMitigationResponse();
            response.setMitigationId(mitigationId);
            response.setPreviousOwnerARN(previousOwnerARN);
            return response;
        }
    }
    
    /**
     * Validate the resource Sets
     * @param resourceMap Map of ResourceType to Set of resources.
     */
    private void validateResources(Map<BlackWatchMitigationResourceType, Set<String>> resourceMap) {
        //For now, only validate IPAddresses.
        Set<String> ipAddresses = resourceMap.getOrDefault(BlackWatchMitigationResourceType.IPAddress, 
                new HashSet<String>());
        Validate.inclusiveBetween(0, MAX_BW_IPADDRESSES, ipAddresses.size());
        ipAddresses.forEach(f -> dogfishHelper.validateCIDRInRegion(f));
    }

    @Override
    public ApplyBlackWatchMitigationResponse applyBlackWatchMitigation(String resourceId, String resourceTypeString,
            Integer minsToLive, MitigationActionMetadata metadata, BlackWatchTargetConfig targetConfig,
            String userARN, TSDMetrics tsdMetrics) {
        Validate.notNull(resourceId);
        Validate.notNull(resourceTypeString);
        Validate.notNull(targetConfig);
        Validate.notNull(userARN);
        Validate.notNull(tsdMetrics);
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

            String canonicalResourceId = typeValidator.getCanonicalStringRepresentation(resourceId);
            String mitigationSettingsJSON = targetConfig.getJsonString();
            Map<BlackWatchMitigationResourceType, Set<String>> resourceMap = 
                    typeValidator.getCanonicalMapOfResources(resourceId, targetConfig);
            LOG.info(String.format("Extracted canonical resource:%s and resource sets:%s",
                    canonicalResourceId, ReflectionToStringBuilder.toString(resourceMap)));
            validateResources(resourceMap);
            ResourceAllocationState resourceState = resourceAllocationStateDynamoDBHelper
                    .getResourceAllocationState(canonicalResourceId);
            
            MitigationState mitigationState;
            if (resourceState != null) {
                Validate.isTrue(resourceState.getResourceType().equals(resourceTypeString), String.format(
                        "Recorded resourceId:%s with type:%s does not match the specified type:%s", canonicalResourceId,
                        resourceState.getResourceType(), resourceTypeString));
                newMitigationCreated = false;
                mitigationId = resourceState.getMitigationId();
                mitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
                if (mitigationState == null) {
                    String message = String.format("MitigationId:%s returned from the resource does not exist!", 
                            mitigationId);
                    throw new IllegalArgumentException(message);
                }
                if (!mitigationState.getOwnerARN().equals(userARN)) {
                    String message = String.format("Cannot apply update to mitigationId:%s as the calling owner:%s "
                            + "does not match the recorded owner:%s", mitigationId, userARN, mitigationState.getOwnerARN());
                    throw new IllegalArgumentException(message);
                }
            } else {
                newMitigationCreated = true;
                mitigationId = generateMitigationId(realm);
                mitigationState = MitigationState.builder()
                        .mitigationId(mitigationId)
                        .resourceId(canonicalResourceId)
                        .resourceType(resourceTypeString)
                        .state(MitigationState.State.Active.name())
                        .ownerARN(userARN)
                        .build();
            }            
            mitigationState.setChangeTime(System.currentTimeMillis());
            mitigationState.setMitigationSettingsJSON(mitigationSettingsJSON);
            mitigationState.setMitigationSettingsJSONChecksum(
                    BlackWatchHelper.getHexStringChecksum(mitigationSettingsJSON));
            mitigationState.setMinutesToLive(minsToLive);
            BlackWatchMitigationActionMetadata bwMetadata = BlackWatchHelper.coralMetadataToBWMetadata(metadata);
            mitigationState.setLatestMitigationActionMetadata(bwMetadata);
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
            }
    
            ApplyBlackWatchMitigationResponse response = new ApplyBlackWatchMitigationResponse();
            response.setNewMitigationCreated(newMitigationCreated);
            response.setMitigationId(mitigationId);
            return response;
        }
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
        try {
            mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(mitigationState, saveExpression);
        } catch (ConditionalCheckFailedException conEx) {
            String msg;
            if (newMitigationCreated) {
                msg = "Could not save MitigationState due to conditional failure! "
                        + "Conflicting MitigationId on a new mitigation.";
            } else {
                msg = String.format("Could not save MitigationState due to conditional failure! "
                        + "MitigationState must not be: %s", MitigationState.State.To_Delete.name());
            }
            throw new IllegalArgumentException(msg);
        } finally {
            subMetrics.addCount("NewMitgationCreated", newMitigationCreated ? 1 : 0);
            subMetrics.addCount("ExistingMitgationModified", newMitigationCreated ? 0 : 1);
        }
    }

    private void failDeactivateMitigation(final String mitigationId, final int attempts) {
        final String msg = String.format("Failed to deactivate mitigationId=%s after %d retries.",
                mitigationId, attempts);
        throw new RuntimeException(msg);
    }

    public void deactivateMitigation(final String mitigationId, final MitigationActionMetadata actionMetadata) {
        final String To_Delete_State = MitigationState.State.To_Delete.name();

        final BlackWatchMitigationActionMetadata actionMetadataBlackWatch =
                BlackWatchHelper.coralMetadataToBWMetadata(actionMetadata);

        // Allow a few retries to avoid clashing with the workers
        for (int attempt = 0; attempt < MAX_UPDATE_RETRIES; attempt++) {
            MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);

            if (state == null) {
                throw new IllegalArgumentException("Specified mitigation Id " + mitigationId + " does not exist");
            } else if (state.getState().equals(To_Delete_State)) {
                return;  // Already in the desired state
            }

            state.setState(To_Delete_State);
            state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);

            try {
                mitigationStateDynamoDBHelper.updateMitigationState(state);
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
}

