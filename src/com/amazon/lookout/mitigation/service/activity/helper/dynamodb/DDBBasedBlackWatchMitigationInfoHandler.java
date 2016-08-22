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

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

public class DDBBasedBlackWatchMitigationInfoHandler implements BlackWatchMitigationInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedBlackWatchMitigationInfoHandler.class);

    private final int totalSegments = 4;

    private MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper;
    private static final String QUERY_BLACKWATCH_MITIGATION_FAILURE = "QUERY_BLACKWATCH_MITIGATION_FAILED";

    @ConstructorProperties({ "mitigationStateDynamoDBHelper" })
    public DDBBasedBlackWatchMitigationInfoHandler(@NonNull MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper) {
        this.mitigationStateDynamoDBHelper = mitigationStateDynamoDBHelper;
    }

    private LocationMitigationStateSettings convertLocationStateSettings(MitigationState.Setting mitigationSettins) {
        return LocationMitigationStateSettings.builder()
                .withBPS(mitigationSettins.getBPS())
                .withPPS(mitigationSettins.getPPS())
                .withMitigationSettingsJSONChecksum(mitigationSettins.getMitigationSettingsJSONChecksum())
                .build();
    }
    
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
                
                List<MitigationState> mitigationStates = mitigationStateDynamoDBHelper.getMitigationState(scanExpression, totalSegments);
                
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

}