package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.aws.authruntimeclient.internal.common.collect.ImmutableMap;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.ddb.model.ActiveMitigationsModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.status.helper.ActiveMitigationsStatusHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Sets;

public class DDBBasedListMitigationsHandler extends DDBBasedRequestStorageHandler implements RequestInfoHandler, ActiveMitigationInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedListMitigationsHandler.class);
    
    // Num Attempts + Retry Sleep Configs.
    public static final int DDB_ACTIVITY_MAX_ATTEMPTS = 3;
    private static final int DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Keys for TSDMetric properties.
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_SUMMARY = "NumGetMitigationDescriptionAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_NAME_AND_REQUEST_STATUS = "NumGetMitigationNameAndRequestStatusAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE = "NumGetActiveMitigationsForServiceAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_INFO = "NumGetMitigationRequestInfo";

    private static final Integer MITIGATION_HISTORY_EVALUCATE_ITEMS_COUNT_PER_QUERY_BASE = 25;
    
    protected final String mitigationRequestsTableName;
    
    private final ActiveMitigationsStatusHelper activeMitigationStatusHelper;
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();
    private final DynamoDB dynamoDB;
    private final Table requestsTable;

    public DDBBasedListMitigationsHandler(AmazonDynamoDBClient dynamoDBClient, String domain, @NonNull ActiveMitigationsStatusHelper activeMitigationStatusHelper) {
        super(dynamoDBClient, domain);
        this.activeMitigationStatusHelper = activeMitigationStatusHelper;
        this.dynamoDB = new DynamoDB(dynamoDBClient);
        
        mitigationRequestsTableName = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.requestsTable = dynamoDB.getTable(mitigationRequestsTableName);
    }

    /**
     * Get an instance of MitigationRequestDescription. This object will contain mitigation request metadata and mitigation definition information.
     * @param deviceName The name of the device you wish to query for.
     * @param jobId The id of the job you wish to query for.
     * @param tsdMetrics A TSDMetrics object.
     * @return An instance of MitigationRequestDescription, describing mitigation request corresponding to the parameters above.
     */
    @Override
    public MitigationRequestDescription getMitigationRequestDescription(String deviceName, long jobId, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(jobId > 0);
        Validate.notNull(tsdMetrics);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDescription");
        try {
            Map<String, AttributeValue> key = generateRequestInfoKey(deviceName, jobId);
            GetItemResult result = getRequestInDDB(key, subMetrics);
            Map<String, AttributeValue> item = result.getItem();
            if (CollectionUtils.isEmpty(item)) {
                return new MitigationRequestDescription();
            }
            return convertToRequestDescription(item);
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the mitigation request associated with the device: " + deviceName + " and jobId: " + jobId;
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
    public MitigationNameAndRequestStatus getMitigationNameAndRequestStatus(String deviceName, String templateName, long jobId, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.notEmpty(templateName);
        Validate.isTrue(jobId > 0);
        Validate.notNull(tsdMetrics); 
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationNameAndRequestStatus");
        try {
            Map<String, AttributeValue> key = generateRequestInfoKey(deviceName, jobId);
            Map<String, AttributeValue> item;
            try {
                GetItemResult result = getRequestInDDB(key, subMetrics);
                item = result.getItem();
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
            }

            // Throw an IllegalArgumentException if the result of the query,i.e, no requests were found.
            if (CollectionUtils.isEmpty(item)) {
                String msg = String.format("Could not find an item for the requested jobId %d, device name %s, and template %s", jobId, deviceName, templateName);
                LOG.info(msg);
                throw new IllegalStateException(msg);
            } 

            String resultTemplateName = item.get(MITIGATION_TEMPLATE_KEY).getS();
            if (!templateName.equals(resultTemplateName)) {
                String msg = String.format("The request associated with job id: %s is associated with a different template than requested: %s", jobId, templateName);
                LOG.info(msg + " Expected template: " + resultTemplateName);
                throw new IllegalStateException(msg);
            }
            
            String mitigationName = item.get(MITIGATION_NAME_KEY).getS();
            String requestStatus = item.get(WORKFLOW_STATUS_KEY).getS();
            
            return new MitigationNameAndRequestStatus(mitigationName, requestStatus);
        } finally {
            subMetrics.addOne(NUM_ATTEMPTS_TO_GET_MITIGATION_NAME_AND_REQUEST_STATUS);
            subMetrics.end();
        }
    }
    
    @Override
    public List<ActiveMitigationDetails> getActiveMitigationsForService(@NonNull String serviceName, String deviceName, List<String> locations, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getActiveMitigationsForService");
        try {
            String tableName = activeMitigationsTableName;
            String indexToUse = ActiveMitigationsModel.DEVICE_NAME_INDEX;
            
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = generateKeyConditionsForServiceAndDevice(serviceName, deviceName);
            
            // Generate query filters to use when querying.
            Map<String, Condition> queryFilter = new HashMap<>();
            queryFilter.put(ActiveMitigationsModel.DELETION_DATE_KEY, new Condition().withComparisonOperator(ComparisonOperator.NULL));
            queryFilter.put(ActiveMitigationsModel.DEFUNCT_DATE_KEY, new Condition().withComparisonOperator(ComparisonOperator.NULL));
            
            // If we have locations, then add locations as a query filter.
            if (!CollectionUtils.isEmpty(locations)) {
                queryFilter.putAll(generateMitigationLocationQueryFilter(Sets.newHashSet(locations)));
            }
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            Set<String> attributes = Sets.newHashSet(ActiveMitigationsModel.MITIGATION_NAME_KEY, ActiveMitigationsModel.LOCATION_KEY, ActiveMitigationsModel.JOB_ID_KEY, 
                                                     ActiveMitigationsModel.DEVICE_NAME_KEY, ActiveMitigationsModel.MITIGATION_VERSION_KEY, ActiveMitigationsModel.LAST_DEPLOY_DATE);
            QueryRequest queryRequest = generateQueryRequest(attributes, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
            
            // Populate the items fetched after each query succeeds.
            List<Map<String, AttributeValue>> fetchedItems = new ArrayList<>();
            
            // Query DDB until there are no more items to fetch (i.e. when lastEvaluatedKey is null)
            do {
                int numAttempts = 0;
                Throwable lastCaughtException = null;
                while (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    ++numAttempts;
                    subMetrics.addOne(NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE);
                    
                    try {
                        queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                        QueryResult result = queryRequestsInDDB(queryRequest, subMetrics);
                        if (result.getCount() > 0) {
                            fetchedItems.addAll(result.getItems());
                        }
                        lastEvaluatedKey = result.getLastEvaluatedKey();
                        break;
                    } catch (Exception ex) {
                        lastCaughtException = ex;
                        String msg = "Caught exception when querying active mitigations associated with service: " + serviceName + " on device: " + deviceName + 
                                     ", ddbQuery: " + ReflectionToStringBuilder.toString(queryRequest);
                        LOG.warn(msg, ex);
                    }
                    
                    if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
                
                if (numAttempts >= DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = "Unable to query active mitigations associated with service: " + serviceName + " on device: " + deviceName +  
                                 ReflectionToStringBuilder.toString(queryRequest) + " after " + numAttempts + " attempts";
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
            } while (lastEvaluatedKey != null);
            
            // Convert the fetched items into a list of ActiveMitigationDetails.
            return activeMitigationsListConverter(fetchedItems);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Fetches a list of MitigationRequestDescriptionWithLocations for each mitigation which is currently being worked on (whose WorkflowStatus is RUNNING).
     * @param serviceName Service for which we need to fetch the mitigation request description for ongoing requests.
     * @param deviceName DeviceName for which we need to fetch the mitigation request description for ongoing requests.
     * @param tsdMetrics
     * @return a List of MitigationRequestDescription, where each MitigationRequestDescription instance describes a mitigation that is currently being worked on (whose WorkflowStatus is RUNNING).
     */
    @Override
    public List<MitigationRequestDescriptionWithLocations> getOngoingRequestsDescription(@NonNull String serviceName, @NonNull String deviceName, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getInProgressRequestsDescription");
        try {
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = generateKeyConditionsForUneditedRequests(deviceName);
            
            // Generate query filters to use when querying.
            Map<String, Condition> queryFilter = new HashMap<>();
            Condition runningWorkflowCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                                .withAttributeValueList(new AttributeValue(WorkflowStatus.RUNNING));
            queryFilter.put(WORKFLOW_STATUS_KEY, runningWorkflowCondition);
 
            Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                            .withAttributeValueList(new AttributeValue(serviceName));
            queryFilter.put(SERVICE_NAME_KEY, serviceNameCondition);
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            QueryRequest queryRequest = generateQueryRequest(null, keyConditions, queryFilter, mitigationRequestsTableName,
                    true, UNEDITED_MITIGATIONS_LSI_NAME, lastEvaluatedKey);
            
            List<MitigationRequestDescriptionWithLocations> descriptions = new ArrayList<>();
            // Query DDB until there are no more items to fetch (i.e. when lastEvaluatedKey is null)
            do {
                int numAttempts = 0;
                Throwable lastCaughtException = null;
                while (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    ++numAttempts;
                    subMetrics.addOne(NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE);
                    
                    try {
                        queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                        QueryResult result = queryRequestsInDDB(queryRequest, subMetrics);
                        if (result.getCount() > 0) {
                            for (Map<String, AttributeValue> item : result.getItems()) {
                                MitigationRequestDescription description = convertToRequestDescription(item);
                                
                                MitigationRequestDescriptionWithLocations descriptionWithLocations = new MitigationRequestDescriptionWithLocations();
                                descriptionWithLocations.setMitigationRequestDescription(description);
                                
                                descriptionWithLocations.setLocations(item.get(LOCATIONS_KEY).getSS());
                                descriptions.add(descriptionWithLocations);
                            }
                        }
                        lastEvaluatedKey = result.getLastEvaluatedKey();
                        break;
                    } catch (Exception ex) {
                        lastCaughtException = ex;
                        String msg = "Caught exception when querying currently running mitigations associated with service: " + serviceName + " on device: " + deviceName + 
                                     ", ddbQuery: " + ReflectionToStringBuilder.toString(queryRequest);
                        LOG.warn(msg, ex);
                    }
                    
                    if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
                
                if (numAttempts >= DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = "Unable to query currently running mitigations associated with service: " + serviceName + " on device: " + deviceName +  
                                 ReflectionToStringBuilder.toString(queryRequest) + " after " + numAttempts + " attempts";
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
            } while (lastEvaluatedKey != null);
            
            return descriptions;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Generate an instance of MitigationRequestDescription from a Map of String to AttributeValue.
     * @param keyValues Map of String to AttributeValue.
     * @return A MitigationRequestDescription object.
     */
    private MitigationRequestDescription convertToRequestDescription(Map<String, AttributeValue> keyValues) {
        MitigationRequestDescription mitigationDescription = new MitigationRequestDescription();
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setUser(keyValues.get(USERNAME_KEY).getS());
        mitigationActionMetadata.setDescription(keyValues.get(USER_DESC_KEY).getS());
        mitigationActionMetadata.setToolName(keyValues.get(TOOL_NAME_KEY).getS());
        
        // Related tickets is optional, hence the check.
        if (keyValues.containsKey(RELATED_TICKETS_KEY)) {
            List<String> tickets = keyValues.get(RELATED_TICKETS_KEY).getSS();
            if (!CollectionUtils.isEmpty(tickets)) {
                mitigationActionMetadata.setRelatedTickets(tickets);
            }
        }
        mitigationDescription.setMitigationActionMetadata(mitigationActionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        if (keyValues.containsKey(MITIGATION_DEFINITION_KEY) && !StringUtils.isEmpty(keyValues.get(MITIGATION_DEFINITION_KEY).getS())) {
            mitigationDefinition = jsonDataConverter.fromData(keyValues.get(MITIGATION_DEFINITION_KEY).getS(), MitigationDefinition.class);
        }
        mitigationDescription.setMitigationDefinition(mitigationDefinition);
        
        mitigationDescription.setMitigationTemplate(keyValues.get(MITIGATION_TEMPLATE_KEY).getS());
        mitigationDescription.setDeviceName(keyValues.get(DEVICE_NAME_KEY).getS());
        mitigationDescription.setJobId(Long.parseLong(keyValues.get(WORKFLOW_ID_KEY).getN()));
        mitigationDescription.setMitigationName(keyValues.get(MITIGATION_NAME_KEY).getS());
        mitigationDescription.setMitigationVersion(Integer.parseInt(keyValues.get(MITIGATION_VERSION_KEY).getN()));
        mitigationDescription.setRequestDate(Long.parseLong(keyValues.get(REQUEST_DATE_KEY).getN()));
        mitigationDescription.setRequestStatus(keyValues.get(WORKFLOW_STATUS_KEY).getS());
        mitigationDescription.setDeviceScope(keyValues.get(DEVICE_SCOPE_KEY).getS());
        mitigationDescription.setRequestType(keyValues.get(REQUEST_TYPE_KEY).getS());
        mitigationDescription.setServiceName(keyValues.get(SERVICE_NAME_KEY).getS());
        mitigationDescription.setUpdateJobId(Long.parseLong(keyValues.get(UPDATE_WORKFLOW_ID_KEY).getN()));
        
        return mitigationDescription;
    }
    
    /*
     * Same as above function, handle new Dynamodb query return type Item
     */
    private MitigationRequestDescription convertToRequestDescription(Item item) {
        MitigationRequestDescription mitigationDescription = new MitigationRequestDescription();
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setUser(item.getString(USERNAME_KEY));
        mitigationActionMetadata.setDescription(item.getString(USER_DESC_KEY));
        mitigationActionMetadata.setToolName(item.getString(TOOL_NAME_KEY));
        
        // Related tickets is optional, hence the check.
        Set<String> tickets = item.getStringSet(RELATED_TICKETS_KEY);
        if (!CollectionUtils.isEmpty(tickets)) {
            mitigationActionMetadata.setRelatedTickets(
                    new ArrayList<String>(tickets));
        }
        mitigationDescription.setMitigationActionMetadata(mitigationActionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        if (!StringUtils.isEmpty(item.getString(MITIGATION_DEFINITION_KEY))) {
            mitigationDefinition = jsonDataConverter.fromData(item.getString(MITIGATION_DEFINITION_KEY), MitigationDefinition.class);
        }
        mitigationDescription.setMitigationDefinition(mitigationDefinition);
        
        mitigationDescription.setMitigationTemplate(item.getString(MITIGATION_TEMPLATE_KEY));
        mitigationDescription.setDeviceName(item.getString(DEVICE_NAME_KEY));
        mitigationDescription.setJobId(item.getLong(WORKFLOW_ID_KEY));
        mitigationDescription.setMitigationName(item.getString(MITIGATION_NAME_KEY));
        mitigationDescription.setMitigationVersion(item.getInt(MITIGATION_VERSION_KEY));
        mitigationDescription.setRequestDate(item.getLong(REQUEST_DATE_KEY));
        mitigationDescription.setRequestStatus(item.getString(WORKFLOW_STATUS_KEY));
        mitigationDescription.setDeviceScope(item.getString(DEVICE_SCOPE_KEY));
        mitigationDescription.setRequestType(item.getString(REQUEST_TYPE_KEY));
        mitigationDescription.setServiceName(item.getString(SERVICE_NAME_KEY));
        mitigationDescription.setUpdateJobId(item.getLong(UPDATE_WORKFLOW_ID_KEY));
        
        return mitigationDescription;
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
     * Generate the key condition for unedited requests for on a certain deviceName.
     * @param deviceName The name of the device.
     * @return a Map of String to Condition - map to be used as the DDB query key.
     */
    protected Map<String, Condition> generateKeyConditionsForUneditedRequests(String deviceName) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Condition deviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                       .withAttributeValueList(new AttributeValue(deviceName));
        keyConditions.put(DDBBasedMitigationStorageHandler.DEVICE_NAME_KEY, deviceNameCondition);
        
        Condition uneditedCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                     .withAttributeValueList(new AttributeValue().withN("0"));
        keyConditions.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, uneditedCondition);
        return keyConditions;
    }
    
    /**
     * Generate a list of MitigationInstanceStatuses from a list of maps of Strings as keys and AttributeValues as values.
     * @param listOfKeyValues A list of maps of Strings as keys and AttributeValues as keys.
     * @return a list of MitigationInstanceStatuses
     */
    private List<ActiveMitigationDetails> activeMitigationsListConverter(List<Map<String, AttributeValue>> listOfKeyValues){ 
        List<ActiveMitigationDetails> convertedList = new ArrayList<>(); 
        for (Map<String, AttributeValue> keyValues : listOfKeyValues) {
            String location = keyValues.get(ActiveMitigationsModel.LOCATION_KEY).getS();
            long jobId = Long.parseLong(keyValues.get(ActiveMitigationsModel.JOB_ID_KEY).getN());
            String mitigationName = keyValues.get(ActiveMitigationsModel.MITIGATION_NAME_KEY).getS();
            String deviceName = keyValues.get(ActiveMitigationsModel.DEVICE_NAME_KEY).getS();
            int mitigationVersion = Integer.parseInt(keyValues.get(ActiveMitigationsModel.MITIGATION_VERSION_KEY).getN());
            
            long lastDeployDate = 0;
            if (keyValues.containsKey(ActiveMitigationsModel.LAST_DEPLOY_DATE)) {
                lastDeployDate = Long.parseLong(keyValues.get(ActiveMitigationsModel.LAST_DEPLOY_DATE).getN());
            }
            
            ActiveMitigationDetails activeMitigationDetails = new ActiveMitigationDetails(mitigationName, jobId, location, 
                                                                                          deviceName, mitigationVersion, lastDeployDate);
            
            convertedList.add(activeMitigationDetails);
        }
        return convertedList;
    }
    
    /**
     * Generates a list of MitigationRequestDescription instances, where each instance wraps information about the latest request.
     * The mitigation must be active.
     * 
     * @param serviceName ServiceName for which we need to get the list of active mitigation descriptions.
     * @param deviceName DeviceName from which we need to read the list of active mitigation descriptions.
     * @param deviceScope DeviceScope to constraint the query to get the list of active mitigation descriptions.
     * @param mitigationName MitigationName constraint for the active mitigations.
     * @param tsdMetrics 
     * @return List of MitigationRequestDescription instances, should contain a single request representing 
     *      the latest (create/edit) mitigation request for this active mitigation.
     * @throws MissingMitigationException400, if mitigation does not exist or has been deleted
     */
    @Override
    public List<MitigationRequestDescription> getActiveMitigationRequestDescriptionsForMitigation(@NonNull String serviceName,
            @NonNull String deviceName, String deviceScope, @NonNull String mitigationName, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(deviceScope);
        Validate.notEmpty(mitigationName);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDescriptionsForMitigation");
        try {
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = ImmutableMap.of(
                    DEVICE_NAME_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue(deviceName)),
                    MITIGATION_NAME_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue(mitigationName)));
            
            // Generate query filters to use when querying.
            Map<String, Condition> queryFilter = ImmutableMap.of(
                    // Restrict results to active mitigation, which means updateWorkflowId == 0
                    UPDATE_WORKFLOW_ID_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue().withN(String.valueOf(UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS))),
                    // Restrict results to this serviceName
                    SERVICE_NAME_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue(serviceName)),
                    // Restrict results to this deviceName
                    DEVICE_SCOPE_KEY, new Condition().withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue(deviceScope)),
                    // Ignore the delete requests, since they don't contain any mitigation definitions.
                    REQUEST_TYPE_KEY, new Condition().withComparisonOperator(ComparisonOperator.NE)
                        .withAttributeValueList(new AttributeValue(RequestType.DeleteRequest.name())));
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            QueryRequest queryRequest = generateQueryRequest(null, keyConditions, queryFilter, mitigationRequestsTableName,
                    true, MitigationRequestsModel.MITIGATION_NAME_LSI, lastEvaluatedKey);
            
            // Populate the items fetched after each query succeeds.
            List<Map<String, AttributeValue>> fetchedItems = new ArrayList<>();
            
            // Query DDB until there are no more items to fetch (i.e. when lastEvaluatedKey is null)
            do {
                int numAttempts = 0;
                Throwable lastCaughtException = null;
                
                // Attempt to query DDB for a fixed number of times, 
                while (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    ++numAttempts;
                    subMetrics.addOne(NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_INFO);
                    
                    try {
                        queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                        QueryResult result = queryRequestsInDDB(queryRequest, subMetrics);
                        fetchedItems.addAll(result.getItems());
                        lastEvaluatedKey = result.getLastEvaluatedKey();
                        break;
                    } catch (Exception ex) {
                        lastCaughtException = ex;
                        String msg = "Caught exception when querying mitigation request info associated with service: " + serviceName + " on device: " + deviceName + 
                                     ", ddbQuery: " + ReflectionToStringBuilder.toString(queryRequest);
                        LOG.warn(msg, ex);
                    }
                    
                    if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
                
                if (numAttempts >= DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = "Unable to query mitigation requests info associated with service: " + serviceName + " on device: " + deviceName +  
                                 ReflectionToStringBuilder.toString(queryRequest) + " after " + numAttempts + " attempts";
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
            } while (lastEvaluatedKey != null);
            
            // Get the latest MitigationRequestDescription from the list of fetchedItems.
            if (!fetchedItems.isEmpty()) {
                return fetchedItems.stream().map(item -> convertToRequestDescription(item)).collect(Collectors.toList());
            }
            throw new MissingMitigationException400("Mitigation: " + mitigationName + " for service: "
                   + serviceName + " doesn't exist on device: " + deviceName + " with deviceScope:" + deviceScope);
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Get mitigation definition based on device name, mitigation name, and mitigation version
     * This API queries GSI, result is eventually consistent.
     * @param deviceName : device name
     * @param mitigationName : mitigation name
     * @param mitigationVersion : mitigation version
     * @param tsdMetrics : TSD metrics
     * @return MitigationRequestDescription
     * @throws MissingMitigationException400, if mitigation not found
     */
    @Override
    public MitigationRequestDescription getMitigationDefinition(String deviceName, String mitigationName, int mitigationVersion,
            TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.notEmpty(mitigationName);
        Validate.isTrue(mitigationVersion > 0);

        QuerySpec query = new QuerySpec().withHashKey(MITIGATION_NAME_KEY, mitigationName)
                .withRangeKeyCondition(new RangeKeyCondition(MITIGATION_VERSION_KEY).eq(mitigationVersion))
                .withQueryFilters(new QueryFilter(DEVICE_NAME_KEY).eq(deviceName),
                        new QueryFilter(REQUEST_TYPE_KEY).ne(RequestType.DeleteRequest.name()),
                        new QueryFilter(WORKFLOW_STATUS_KEY).in(WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING))
                .withMaxResultSize(10);
        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDefinition")) {
            try {
                for (Item item : requestsTable.getIndex(MitigationRequestsModel
                        .MITIGATION_NAME_MITIGATION_VERSION_GSI).query(query)) {
                    return convertToRequestDescription(item);
                }
            } catch (Exception ex) {
                String msg = "Unable to query mitigation requests info associated on device: " + deviceName
                        + ReflectionToStringBuilder.toString(query);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            throw new MissingMitigationException400("Mitigation: " + mitigationName
                    + " doesn't exist on device: " + deviceName);
        }
    }

    /**
     * Get mitigation history for a mitigation.
     * For example: a mitigation has version 1, 2, 3, 4, 5
     * The returned order will be 5, 4, 3, 2, 1, from the most recent to least recent.
     * The start version is used to paginate the return history result. If start version is 3, the first version returned will be 2.
     * The maxNumberOfHistoryEntriesToFetch controls the max number of history entries to return.
     * This API queries GSI, result is eventually consistent.
     * @param serviceName : service name of the mitigation
     * @param deviceName : device name of the mitigation
     * @param deviceScope : device scope of the mitigation
     * @param mitigationName : mitigation name of the mitigation
     * @param exclusiveStartVersion : the start version of history retrieval, result will not include this version
     * @param maxNumberOfHistoryEntriesToFetch : the max number of history entries to retrieve
     * @param tsdMetrics : TSD metric
     * @return : list of history entries of a mitigation.
     * @throws : MissingMitigationException400, if mitigation not found
     */
    @Override
    public List<MitigationRequestDescription> getMitigationHistoryForMitigation(
            String serviceName, String deviceName, String deviceScope,
            String mitigationName, Integer exclusiveStartVersion, 
            Integer maxNumberOfHistoryEntriesToFetch, TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(deviceScope);
        Validate.notEmpty(mitigationName);
        Validate.notNull(maxNumberOfHistoryEntriesToFetch);
        Validate.notNull(tsdMetrics);
        QuerySpec query = new QuerySpec().withHashKey(MITIGATION_NAME_KEY, mitigationName)
                .withQueryFilters(new QueryFilter(SERVICE_NAME_KEY).eq(serviceName),
                        new QueryFilter(DEVICE_SCOPE_KEY).eq(deviceScope),
                        new QueryFilter(DEVICE_NAME_KEY).eq(deviceName),
                        new QueryFilter(REQUEST_TYPE_KEY).ne(RequestType.DeleteRequest.name()),
                        new QueryFilter(WORKFLOW_STATUS_KEY).in(WorkflowStatus.SUCCEEDED, WorkflowStatus.RUNNING))
                .withMaxResultSize(maxNumberOfHistoryEntriesToFetch
                        // Max result size also decides how many items to evaluate in one query.
                        // To reduce the number of under-layer call to DDB service, we add a small overhead
                        // to accommodate the filtered items.
                        + MITIGATION_HISTORY_EVALUCATE_ITEMS_COUNT_PER_QUERY_BASE)
                .withScanIndexForward(false)
                .withConsistentRead(false);
                
        if (exclusiveStartVersion != null) {
            query.withRangeKeyCondition(new RangeKeyCondition(MITIGATION_VERSION_KEY).lt(exclusiveStartVersion));
        }
        
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics(
                "DDBBasedListMitigationsHandler.getMitigationHistoryForMitigation")) {
                List<MitigationRequestDescription> descs = new ArrayList<>(maxNumberOfHistoryEntriesToFetch);
            try {
                for (Item item : requestsTable.getIndex(MitigationRequestsModel.MITIGATION_NAME_MITIGATION_VERSION_GSI)
                        .query(query)) {
                    descs.add(convertToRequestDescription(item));
                    if (descs.size() >= maxNumberOfHistoryEntriesToFetch) {
                        break;
                    }
                }
            } catch (Exception ex) {
                String msg = "Failed to query mitigation requests to get mitigation history, query: "
                        + ReflectionToStringBuilder.toString(query);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            if (!descs.isEmpty()) {
                return descs;
            }
            throw new MissingMitigationException400("Mitigation: " + mitigationName + " for service: " + serviceName +
                    " doesn't exist on device: " + deviceName + " with deviceScope:" + deviceScope);
        }
    }
    
    /**
     * 
     * @param serviceName Service whose mitigation needs to be marked defunct.
     * @param mitigationName Mitigation which needs to be marked defunct.
     * @param deviceName Device on which the mitigation to be marked defunct exists.
     * @param location Location where this mitigation should be marked as defunct.
     * @param jobId WorkflowId responsible for creating the mitigation which now needs to be marked as defunct.
     * @param lastDeployDate Last date when this mitigation was modified, required to ensure the mitigation hasn't changed since the caller requested this mitigation to be marked as defunct.
     * @param tsdMetrics
     * @return UpdateItemResult as a result of performing DDB update.
     * @throws ConditionalCheckFailedException
     * @throws AmazonClientException
     * @throws AmazonServiceException
     */
    @Override
    public UpdateItemResult markMitigationAsDefunct(@NonNull String serviceName, @NonNull String mitigationName, @NonNull String deviceName, @NonNull String location, 
                                                    long jobId, long lastDeployDate, @NonNull TSDMetrics tsdMetrics) throws ConditionalCheckFailedException, AmazonClientException, AmazonServiceException {
        return activeMitigationStatusHelper.markMitigationAsDefunct(serviceName, mitigationName, deviceName, location, jobId, lastDeployDate, tsdMetrics);
    }
}
