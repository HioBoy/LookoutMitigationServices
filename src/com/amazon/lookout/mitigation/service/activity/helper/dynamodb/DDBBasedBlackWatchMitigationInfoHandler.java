package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.ipset.SubnetBasedIpSet;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.arn.ARN;
import com.amazon.arn.ARNSyntaxException;
import com.amazon.blackwatch.helper.BlackWatchHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressResourceTypeValidator;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationState.State;
import com.amazon.blackwatch.mitigation.state.model.MitigationStateSetting;
import com.amazon.blackwatch.mitigation.state.model.ResourceAllocationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchResourceTypeValidator;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationNotOwnedByRequestor400;
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

    private static final String DEFAULT_SHAPER_NAME = "default";

    private static final String QUERY_BLACKWATCH_MITIGATION_FAILURE = "QUERY_BLACKWATCH_MITIGATION_FAILED";

    private static final int MAX_BW_IPADDRESSES = 256;

    private static final int MAX_UPDATE_RETRIES = 3;
    private static final int UPDATE_RETRY_SLEEP_MILLIS = 100;
    private static final long REFRESH_PERIOD_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private List<MitigationState> mitigationStateList;
    private long lastUpdateTimestamp = 0;
    
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
            mitigationState.setState(MitigationState.State.Active.name());
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

            // Supporting 'ARN, IP Address' format for resources
            String ipAddress = null;
            if (resourceType.equals(BlackWatchMitigationResourceType.ElasticIP)) {
                String[] resourceData = resourceId.split(",");
                if (resourceData.length != 2) {
                    throw new IllegalArgumentException("Resource ID must be 'ARN,EIP' format for now!");
                }
                resourceId = resourceData[0];
                ipAddress = resourceData[1];
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
                }
                if (!mitigationState.getOwnerARN().equals(userARN)) {
                    String message = String.format("Cannot apply update to mitigationId:%s as the calling owner:%s "
                            + "does not match the recorded owner:%s", mitigationId, userARN, mitigationState.getOwnerARN());
                    throw new MitigationNotOwnedByRequestor400(message);
                }
            } else {
                newMitigationCreated = true;
                mitigationId = generateMitigationId(realm);
                mitigationState = MitigationState.builder()
                        .mitigationId(mitigationId)
                        .resourceId(canonicalResourceId)
                        .resourceType(resourceTypeString)
                        .ownerARN(userARN)
                        .build();
            }

            // Need to prevent auto-mitigations from BAM and EC2 from creating more specific
            // mitigations within the IP space already covered by existing mitigations.
            // Route53 creates and maintains long-lasting mitigations on larger IP prefixes (/22).
            if (userARN.contains(bamAndEc2OwnerArnPrefix) && ipAddress != null) {
                final String ipAddressToCheck = ipAddress;

                if (lastUpdateTimestamp + REFRESH_PERIOD_MILLIS < System.currentTimeMillis()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Updating the mitigation state list...");
                    }
                    mitigationStateList = mitigationStateDynamoDBHelper.getAllMitigationStates(2);
                    lastUpdateTimestamp = System.currentTimeMillis();
                }

                Optional<MitigationState> mitigationStateWithSupersetPrefix = mitigationStateList.stream()
                        .filter(ms -> ms.getState().equals(State.Active.name()))
                        .filter(ms -> !ms.getOwnerARN().equals(userARN))
                        .filter(ms -> isRequestIpCoveredByExistingMitigation(ipAddressToCheck, ms))
                        .findAny();

                if (mitigationStateWithSupersetPrefix.isPresent()) {
                    String errorMsg = String.format("The request is rejected since the user %s is "
                            + "auto mitigation (BAM or EC2), and mitigation %s already exists on a superset prefix",
                            userARN,
                            mitigationStateWithSupersetPrefix.get().getMitigationId());
                    LOG.warn(errorMsg);
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
            ApplyBlackWatchMitigationResponse response = new ApplyBlackWatchMitigationResponse();
            response.setNewMitigationCreated(newMitigationCreated);
            response.setMitigationId(mitigationId);
            return response;
        }
    }

    private boolean isRequestIpCoveredByExistingMitigation(
            final String ipAddress,
            final MitigationState mitigationState) {
        return mitigationState.getRecordedResources().get(BlackWatchMitigationResourceType.IPAddress.name())
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
}

