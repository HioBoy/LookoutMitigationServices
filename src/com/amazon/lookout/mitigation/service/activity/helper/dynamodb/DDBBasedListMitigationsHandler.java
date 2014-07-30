package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.activities.model.MitigationMetadata;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Sets;

public class DDBBasedListMitigationsHandler extends DDBBasedRequestStorageHandler implements RequestInfoHandler, ActiveMitigationInfoHandler{
    private static final Log LOG = LogFactory.getLog(DDBBasedListMitigationsHandler.class);
    
    public static final String LOCATION_KEY = "Location";
    public static final String JOB_ID_KEY = "JobId";
    public static final String DEVICE_NAME_INDEX = "DeviceName-index";
    
    // Keys for TSDMetric property.s
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_SUMMARY = "NumGetMitigationMetadataAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_NAME_AND_REQUEST_STATUS = "NumGetMitigationNameAndRequestStatusAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE = "NumGetActiveMitigationsForServiceAttempts";
    
    protected final String mitigationInstancesTableName;
    protected final String mitigationRequestsTableName;
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();

    public DDBBasedListMitigationsHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain) {
        super(dynamoDBClient, domain);
        
        mitigationInstancesTableName = DDBBasedMitigationStorageHandler.MITIGATION_INSTANCES_TABLE_NAME_PREFIX + domain.toUpperCase();
        mitigationRequestsTableName = DDBBasedRequestStorageHandler.MITIGATION_REQUEST_TABLE_NAME_PREFIX + domain.toUpperCase();
    }

    /**
     * Get an instance of MitigationMetadata. This object will contain mitigation request metadata and mitigation definition information.
     * @param deviceName The name of the device you wish to query for.
     * @param jobId The id of the job you wish to query for.
     * @param tsdMetrics A TSDMetrics object.
     * @return A MitigationMetadata object.
     */
    @Override
    public MitigationMetadata getMitigationMetadata(String deviceName, long jobId, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(jobId > 0);
        Validate.notNull(tsdMetrics);
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationMetadata");
        try {
            Map<String, AttributeValue> key = generateRequestInfoKey(deviceName, jobId);
            GetItemResult result = getRequestInDDB(key, subMetrics);
            Map<String, AttributeValue> item = result.getItem();
            if (CollectionUtils.isEmpty(item)) {
                return new MitigationMetadata();
            }
            return mitigationMetadataConverter(item);
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the mitigation request associated with the device: " + deviceName 
                    + " and jobId: " + jobId;
            LOG.warn(msg, ex);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addOne(NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_SUMMARY);
            subMetrics.end();
        }
    }
    
    /**
     * Get the mitigation name and request status of the item with the provided deviceName and jobId
     * @param deviceName The name of the device where the mitigation was deployed to.
     * @param jobId The id of the job related to the mitigation whos name you want.
     * @return A MitigationNameAndRequestStatus object which contains the name and job status 
     * of the name of the mitigation you want.
     */
    @Override
    public MitigationNameAndRequestStatus getMitigationNameAndRequestStatus(String deviceName, long jobId, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(jobId > 0);
        Validate.notNull(tsdMetrics); 
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationNameAndRequestStatus");
        try {
            Map<String, AttributeValue> key = generateRequestInfoKey(deviceName, jobId);
            GetItemResult result = getRequestInDDB(key, subMetrics);
            Map<String, AttributeValue> item = result.getItem();
            // Throw an IllegalArgumentException if the result of the query,i.e, no requests were found.
            if (CollectionUtils.isEmpty(item)) {
                throw new IllegalArgumentException();
            }
            
            String mitigationName = item.get(MITIGATION_NAME_KEY).getS();
            String requestStatus = item.get(WORKFLOW_STATUS_KEY).getS();
            
            return new MitigationNameAndRequestStatus(mitigationName, requestStatus);
        } catch (IllegalArgumentException ex) {
            String msg = "The request associated with job id: " + jobId
                    + " and device : " + deviceName + " does not exist";
            LOG.warn(msg, ex);
            throw new IllegalArgumentException(msg);
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the mitigation name associated with job id: " + jobId
                    + " and device : " + deviceName;
            LOG.warn(msg, ex);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addOne(NUM_ATTEMPTS_TO_GET_MITIGATION_NAME_AND_REQUEST_STATUS);
            subMetrics.end();
        }
    }
    
    @Override
    public List<ActiveMitigationDetails> getActiveMitigationsForService(String serviceName, String deviceName, List<String> locations, TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notNull(tsdMetrics);
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getActiveMitigationsForService");
        
        String tableName = activeMitigationsTableName;
        String indexToUse = DEVICE_NAME_INDEX;
        
        Map<String, AttributeValue> lastEvaluatedKey = null;
        QueryResult result = null;
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put(DELETION_DATE_KEY, new Condition().withComparisonOperator(ComparisonOperator.NULL));      
        
        Set<String> attributes = Sets.newHashSet(MITIGATION_NAME_KEY, LOCATION_KEY, JOB_ID_KEY, DEVICE_NAME_KEY, MITIGATION_VERSION_KEY);
        Map<String, Condition> keyConditions = generateKeyConditionsForServiceAndDevice(serviceName, deviceName);
        
        if (!CollectionUtils.isEmpty(locations)) {
            queryFilter.putAll(generateMitigationLocationQueryFilter(Sets.newHashSet(locations)));
        }
        
        QueryRequest queryRequest = generateQueryRequest(attributes, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
        try {
            result = queryRequestsInDDB(queryRequest, subMetrics);
            lastEvaluatedKey = result.getLastEvaluatedKey();
            // If there are still more items to return then query again beginning at the point you left off before.
            while (lastEvaluatedKey != null) {
                queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                QueryResult nextResult = queryRequestsInDDB(queryRequest, subMetrics);
                result.getItems().addAll(nextResult.getItems());
                lastEvaluatedKey = result.getLastEvaluatedKey();
            }
            // Return an empty list if no active mitigations exist.
            if (CollectionUtils.isEmpty(result.getItems())) {
                return new ArrayList<ActiveMitigationDetails>();
            }
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the active mitigations associated with the service: " + serviceName;
            LOG.warn(msg, ex);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addOne(NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE);
            subMetrics.end();
        }
        
        return activeMitigationsListConverter(result.getItems());
    }
    
    /**
     * Generate an instance of MitigationMetadata from a Map of String to AttributeValue.
     * @param item A Maps of Strings to AttributeValue.
     * @return A MitigationMetadata object.
     */
    private MitigationMetadata mitigationMetadataConverter(Map<String, AttributeValue> keyValues){ 
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setUser(keyValues.get(USERNAME_KEY).getS());
        mitigationActionMetadata.setDescription(keyValues.get(USER_DESC_KEY).getS());
        mitigationActionMetadata.setToolName(keyValues.get(TOOL_NAME_KEY).getS());
        if (keyValues.containsKey(RELATED_TICKETS_KEY)) {
            List<String> tickets = keyValues.get(RELATED_TICKETS_KEY).getSS();
            if (!CollectionUtils.isEmpty(tickets)) {
                mitigationActionMetadata.setRelatedTickets(keyValues.get(RELATED_TICKETS_KEY).getSS());
            }
        }
        
        MitigationDefinition mitigationDefinition = jsonDataConverter.fromData(
            keyValues.get(MITIGATION_DEFINITION_KEY).getS(), MitigationDefinition.class);
        
        String mitigationTemplate = keyValues.get(MITIGATION_TEMPLATE_KEY).getS();
        String mitigationName = keyValues.get(MITIGATION_NAME_KEY).getS();
        int mitigationVersion = Integer.valueOf(keyValues.get(MITIGATION_VERSION_KEY).getN());
        long requestDate = Long.valueOf(keyValues.get(REQUEST_DATE_KEY).getN());
        MitigationMetadata mitigationMetadata = new MitigationMetadata(mitigationTemplate,
            mitigationName, mitigationVersion, mitigationDefinition, mitigationActionMetadata, requestDate);
        
        return mitigationMetadata;
    }
    
    /**
     * Generate a set of attributes to serve as keys for our request. Protected for unit test
     * @return Set of string representing the attributes that need to be matched in the DDB Get request.
     */
    protected Map<String, AttributeValue> generateRequestInfoKey(String deviceName, long jobId) {
        Map<String, AttributeValue> attributesInItemToRequest = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(deviceName);
        attributesInItemToRequest.put(DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(jobId));
        attributesInItemToRequest.put(WORKFLOW_ID_KEY, attributeValue);
        
        return attributesInItemToRequest;
    }
    
    /**
     * Generate a QueryRequest object.
     * @param attributesToGet A set of Strings which list the attributes that you want to get.
     * @param keyConditions A map of conditions for your keys. 
     * @param queryFilter A map of conditions to filter your QueryResult.
     * @param tableName The name of the table you wish to query.
     * @param consistentRead Boolean used to determine if you want consistent reads.
     * @param indexName The name of the index you wish to use.
     * @param lastEvaluatedKey A map with the last key that was evaluated. This is used for large queries where
     * you might surpass the 1Mb limit imposed by DynamoDB.
     * @return a QueryRequest object
     */
    private QueryRequest generateQueryRequest(Set<String> attributesToGet, Map<String, Condition> keyConditions,
            Map<String, Condition> queryFilter, String tableName, Boolean consistentRead, String indexName,
            Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setAttributesToGet(attributesToGet);
        queryRequest.setTableName(tableName);
        queryRequest.setConsistentRead(consistentRead);
        queryRequest.setKeyConditions(keyConditions);
        if (queryFilter != null) {
            queryRequest.setQueryFilter(queryFilter);
        }
        if (lastEvaluatedKey != null) {
            queryRequest.setExclusiveStartKey(lastEvaluatedKey);
        }
        if (indexName != null) {
            queryRequest.setIndexName(indexName);
        }
        return queryRequest;
    }
    
    /**
     * Generate a QueryFilter used to filter out results from a QueryResult created after querying the ACTIVE_MITIGATIONS table.
     * @param locations A set of locations
     * @return a Map of Strings to Conditions.
     */
    protected Map<String, Condition> generateMitigationLocationQueryFilter(Set<String> locations) {
        Map<String, Condition> conditions = new HashMap<>();
        Set<AttributeValue> values = new HashSet<>();
        
        if (!locations.isEmpty()) {
            for(String location : locations) {
                AttributeValue value = new AttributeValue(location);
                values.add(value);
            }
            Condition condition = new Condition();
            condition.setAttributeValueList(values);
            condition.setComparisonOperator(ComparisonOperator.IN);
            conditions.put(DDBBasedMitigationStorageHandler.LOCATION_KEY, condition);
        }
        
        return conditions;
    }
    
    /**
     * Generate the key condition for a given serviceName and deviceName.
     * @param deviceName The name of the service.
     * @param serviceName The name of the device.
     * @return a Map of Strings to Conditions. A key condition map.
     */
    protected Map<String, Condition> generateKeyConditionsForServiceAndDevice(String serviceName, String deviceName) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Set<AttributeValue> keyValues = new HashSet<>();
        AttributeValue keyValue = new AttributeValue(serviceName);
        keyValues.add(keyValue);

        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);

        keyConditions.put(DDBBasedMitigationStorageHandler.SERVICE_NAME_KEY, condition);
        
        if (!StringUtils.isEmpty(deviceName)) {
            keyValues = new HashSet<>();
            keyValue = new AttributeValue(deviceName);
            keyValues.add(keyValue);
            
            condition = new Condition();
            condition.setComparisonOperator(ComparisonOperator.EQ);
            condition.setAttributeValueList(keyValues);
            
            keyConditions.put(DDBBasedMitigationStorageHandler.DEVICE_NAME_KEY, condition);
        }
        
        return keyConditions;
    }
    
    /**
     * Generate a list of MitigationInstanceStatuses from a list of maps of Strings as keys and AttributeValues as values.
     * @param list A list of maps of Strings as keys and AttributeValues as keys.
     * @param showCreateDate A boolean indicating whether we want to return the createdate for a mitigation.
     * @return a list of MitigationInstanceStatuses
     */
    private List<ActiveMitigationDetails> activeMitigationsListConverter(List<Map<String, AttributeValue>> listOfKeyValues){ 
        List<ActiveMitigationDetails> convertedList = new ArrayList<>(); 
        for (Map<String, AttributeValue> keyValues : listOfKeyValues) {
            String location = keyValues.get(LOCATION_KEY).getS();
            long jobId = Long.valueOf(keyValues.get(JOB_ID_KEY).getN());
            String mitigationName = keyValues.get(MITIGATION_NAME_KEY).getS();
            String deviceName = keyValues.get(DEVICE_NAME_KEY).getS();
            int mitigationVersion = Integer.valueOf(keyValues.get(MITIGATION_VERSION_KEY).getN());
            
            ActiveMitigationDetails activeMitigationDetails = new ActiveMitigationDetails(mitigationName, jobId,
                    location, deviceName, mitigationVersion);
            
            convertedList.add(activeMitigationDetails);
        }
        return convertedList;
    }
    
}
