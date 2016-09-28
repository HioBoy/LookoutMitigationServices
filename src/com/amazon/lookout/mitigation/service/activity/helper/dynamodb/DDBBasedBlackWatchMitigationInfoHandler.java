package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableMap;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

public class DDBBasedBlackWatchMitigationInfoHandler implements BlackWatchMitigationInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedBlackWatchMitigationInfoHandler.class);

    private int parallelScanSegments;
    private MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper;
    private static final String QUERY_BLACKWATCH_MITIGATION_FAILURE = "QUERY_BLACKWATCH_MITIGATION_FAILED";

    @ConstructorProperties({ "mitigationStateDynamoDBHelper", "parallelScanSegments" })
    public DDBBasedBlackWatchMitigationInfoHandler(@NonNull MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper,
            int parallelScanSegments) {
        this.mitigationStateDynamoDBHelper = mitigationStateDynamoDBHelper;
        this.parallelScanSegments = parallelScanSegments;
    }

    private LocationMitigationStateSettings convertLocationStateSettings(MitigationState.Setting mitigationSettins) {
        return LocationMitigationStateSettings.builder()
                .withBPS(mitigationSettins.getBPS())
                .withPPS(mitigationSettins.getPPS())
                .withMitigationSettingsJSONChecksum(mitigationSettins.getMitigationSettingsJSONChecksum())
                .build();
    }
    
    //TODO: 
    // we need change this method when we decide the strategy for nextToken
    // 1. need pass nextToken as a parameter, parse it and use the parsed result in the db scan request
    // 2. need construct a new nextToken and return it to the caller, so it knows if all items were returned or maxResults was hit.
    @Override
    public List<BlackWatchMitigationDefinition> getBlackWatchMitigations(String mitigationId, String resourceId,
            String resourceType, String ownerARN, long maxNumberOfEntriesToReturn, TSDMetrics tsdMetrics) {
        
        Validate.notNull(tsdMetrics);
        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedBlackWatchMitigationInfoHandler.getBlackWatchMitigations")) {
            List<BlackWatchMitigationDefinition> listOfBlackWatchMitigations = new ArrayList<>();
            try {
                DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                if (mitigationId != null && !mitigationId.isEmpty()) {
                    scanExpression.addFilterCondition(MitigationState.MITIGATION_ID_KEY, 
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(mitigationId)));
                }
                if (resourceId != null && !resourceId.isEmpty()) {
                    scanExpression.addFilterCondition(MitigationState.RESOURCE_ID_KEY, 
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(resourceId)));
                }
                if (resourceType != null && !resourceType.isEmpty()) {
                    scanExpression.addFilterCondition(MitigationState.RESOURCE_TYPE_KEY, 
                            new Condition()
                                .withComparisonOperator(ComparisonOperator.EQ)
                                .withAttributeValueList(new AttributeValue().withS(resourceType)));
                }
                
                List<MitigationState> mitigationStates = mitigationStateDynamoDBHelper.getMitigationState(scanExpression, parallelScanSegments);
                
                for (MitigationState ms : mitigationStates) {
                    MitigationActionMetadata mitigationActionMetadata = MitigationActionMetadata.builder()
                            .withUser(ms.getLatestMitigationActionMetadata().getUser())
                            .withToolName(ms.getLatestMitigationActionMetadata().getToolName())
                            .withDescription(ms.getLatestMitigationActionMetadata().getDescription())
                            .withRelatedTickets(ms.getLatestMitigationActionMetadata().getRelatedTickets())
                            .build();
                    
                    Map<String, LocationMitigationStateSettings> locationMitigationState = ms.getLocationMitigationState().entrySet().stream()
                            .collect(Collectors.toMap(p -> p.getKey(), p -> convertLocationStateSettings(p.getValue())));
                            
                    BlackWatchMitigationDefinition mitigationDefinition = BlackWatchMitigationDefinition.builder()
                            .withMitigationId(ms.getMitigationId())
                            .withResourceId(ms.getResourceId())
                            .withResourceType(ms.getResourceType())
                            .withChangeTime(ms.getChangeTime())
                            .withOwnerARN(ms.getOwnerARN())
                            .withState(ms.getState())
                            .withGlobalPPS(ms.getPpsRate())
                            .withGlobalBPS(ms.getBpsRate())
                            .withMitigationSettingsJSON(ms.getMitigationSettingsJSON())
                            .withMitigationSettingsJSONChecksum(ms.getMitigationSettingsJSONChecksum())
                            .withMinutesToLiveAtChangeTime(ms.getMinutesToLive().intValue())
                            .withExpiryTime(ms.getChangeTime() + ms.getMinutesToLive())
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
                String msg = String.format("Caught exception when querying blackwatch mitigations with mitigationId: %s, resourceId: %s, resourceType: %s, ownerARN: %s",
                        mitigationId, resourceId, resourceType, ownerARN);
                LOG.error(msg, ex);
                subMetrics.addOne(QUERY_BLACKWATCH_MITIGATION_FAILURE);
                throw ex;
            }
            return listOfBlackWatchMitigations;
        }
    }

    public void deactivateMitigation(String mitigationId, MitigationActionMetadata actionMetadata) {
            // Get current state
            String To_Delete_State = MitigationState.State.To_Delete.name();
            MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
            if (state == null) {
                throw new IllegalArgumentException("Specified mitigation Id " + mitigationId + " does not exist");
            }
            state.setState(To_Delete_State);
            com.amazon.blackwatch.mitigation.state.model.MitigationActionMetadata actionMetadataBlackWatch = 
                com.amazon.blackwatch.mitigation.state.model.MitigationActionMetadata.builder()
                .user(actionMetadata.getUser())
                .toolName(actionMetadata.getToolName())
                .description(actionMetadata.getDescription())
                .relatedTickets(actionMetadata.getRelatedTickets())
                .build();
            state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);
            DynamoDBSaveExpression condition = new DynamoDBSaveExpression();
            ExpectedAttributeValue expectedValue = new ExpectedAttributeValue(
                    new AttributeValue(To_Delete_State));
            expectedValue.setComparisonOperator(ComparisonOperator.NE);
            Map<String, ExpectedAttributeValue> expectedAttributes = 
                    ImmutableMap.<String, ExpectedAttributeValue>builder().
                    put(MitigationState.STATE_KEY, expectedValue).
                    build();
            condition.setExpected(expectedAttributes);
            mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(state, condition);
    }

     public void changeOwnerARN(String mitigationId, String newOwnerARN, String expectedOwnerARN, MitigationActionMetadata actionMetadata) {
             // Get current state
             MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
            if (state == null) {
                throw new IllegalArgumentException("Specified mitigation Id " + mitigationId + " does not exist");
            }
            state.setOwnerARN(newOwnerARN);
            com.amazon.blackwatch.mitigation.state.model.MitigationActionMetadata actionMetadataBlackWatch = 
                com.amazon.blackwatch.mitigation.state.model.MitigationActionMetadata.builder()
                .user(actionMetadata.getUser())
                .toolName(actionMetadata.getToolName())
                .description(actionMetadata.getDescription())
                .relatedTickets(actionMetadata.getRelatedTickets())
                .build();
            state.setLatestMitigationActionMetadata(actionMetadataBlackWatch);
             DynamoDBSaveExpression condition = new DynamoDBSaveExpression();
             ExpectedAttributeValue expectedValue = new ExpectedAttributeValue(
                     new AttributeValue(expectedOwnerARN));
             expectedValue.setComparisonOperator(ComparisonOperator.EQ);
             Map<String, ExpectedAttributeValue> expectedAttributes = 
                     ImmutableMap.<String, ExpectedAttributeValue>builder().
                     put(MitigationState.OWNER_ARN_KEY, expectedValue).
                     build();
             condition.setExpected(expectedAttributes);
             mitigationStateDynamoDBHelper.performConditionalMitigationStateUpdate(state, condition);
     }
}
