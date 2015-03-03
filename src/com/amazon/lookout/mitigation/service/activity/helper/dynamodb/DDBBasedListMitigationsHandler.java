package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import lombok.NonNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.ddb.model.ActiveMitigationsModel;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
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
    
    protected final String mitigationInstancesTableName;
    protected final String mitigationRequestsTableName;
    
    private final ActiveMitigationsStatusHelper activeMitigationStatusHelper;
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();

    public DDBBasedListMitigationsHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain, @NonNull ActiveMitigationsStatusHelper activeMitigationStatusHelper) {
        super(dynamoDBClient, domain);
        this.activeMitigationStatusHelper = activeMitigationStatusHelper;
        
        mitigationInstancesTableName = MitigationInstancesModel.MITIGATION_INSTANCES_TABLE_NAME_PREFIX + domain.toUpperCase();
        mitigationRequestsTableName = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase();
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
    public List<ActiveMitigationDetails> getActiveMitigationsForService(@Nonnull String serviceName, String deviceName, List<String> locations, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notNull(tsdMetrics);
        
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
    public List<MitigationRequestDescriptionWithLocations> getOngoingRequestsDescription(@Nonnull String serviceName, @Nonnull String deviceName, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notNull(tsdMetrics);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getInProgressRequestsDescription");
        try {
            String tableName = mitigationRequestsTableName;
            String indexToUse = UNEDITED_MITIGATIONS_LSI_NAME;
            
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
            Set<String> attributes = new HashSet<>(MitigationRequestsModel.getAttributeNamesForRequestTable());
            QueryRequest queryRequest = generateQueryRequest(attributes, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
            
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
     * @param item Map of String to AttributeValue.
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
                mitigationActionMetadata.setRelatedTickets(keyValues.get(RELATED_TICKETS_KEY).getSS());
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
        mitigationDescription.setMitigationVersion(Integer.valueOf(keyValues.get(MITIGATION_VERSION_KEY).getN()));
        mitigationDescription.setRequestDate(Long.valueOf(keyValues.get(REQUEST_DATE_KEY).getN()));
        mitigationDescription.setRequestStatus(keyValues.get(WORKFLOW_STATUS_KEY).getS());
        mitigationDescription.setDeviceScope(keyValues.get(DEVICE_SCOPE_KEY).getS());
        mitigationDescription.setRequestType(keyValues.get(REQUEST_TYPE_KEY).getS());
        mitigationDescription.setServiceName(keyValues.get(SERVICE_NAME_KEY).getS());
        mitigationDescription.setUpdateJobId(Long.parseLong(keyValues.get(UPDATE_WORKFLOW_ID_KEY).getN()));
        
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
     * @param list A list of maps of Strings as keys and AttributeValues as keys.
     * @param showCreateDate A boolean indicating whether we want to return the createdate for a mitigation.
     * @return a list of MitigationInstanceStatuses
     */
    private List<ActiveMitigationDetails> activeMitigationsListConverter(List<Map<String, AttributeValue>> listOfKeyValues){ 
        List<ActiveMitigationDetails> convertedList = new ArrayList<>(); 
        for (Map<String, AttributeValue> keyValues : listOfKeyValues) {
            String location = keyValues.get(ActiveMitigationsModel.LOCATION_KEY).getS();
            long jobId = Long.valueOf(keyValues.get(ActiveMitigationsModel.JOB_ID_KEY).getN());
            String mitigationName = keyValues.get(ActiveMitigationsModel.MITIGATION_NAME_KEY).getS();
            String deviceName = keyValues.get(ActiveMitigationsModel.DEVICE_NAME_KEY).getS();
            int mitigationVersion = Integer.valueOf(keyValues.get(ActiveMitigationsModel.MITIGATION_VERSION_KEY).getN());
            
            long lastDeployDate = 0;
            if (keyValues.containsKey(ActiveMitigationsModel.LAST_DEPLOY_DATE)) {
                lastDeployDate = Long.valueOf(keyValues.get(ActiveMitigationsModel.LAST_DEPLOY_DATE).getN());
            }
            
            ActiveMitigationDetails activeMitigationDetails = new ActiveMitigationDetails(mitigationName, jobId, location, 
                                                                                          deviceName, mitigationVersion, lastDeployDate);
            
            convertedList.add(activeMitigationDetails);
        }
        return convertedList;
    }
    
    /**
     * Generates a list of MitigationRequestDescription instances, where each instance wraps information about an active mitigation request.
     * An active mitigation request is one which hasn't yet been updated by another workflow (there might be another workflow currently working on
     * an update, but if the updating workflow hasn't yet completed, an existing request will be considered as an active request).
     * 
     * @param serviceName ServiceName for which we need to get the list of active mitigation descriptions.
     * @param deviceName DeviceName from which we need to read the list of active mitigation descriptions.
     * @param deviceScope DeviceScope to constraint the query to get the list of active mitigation descriptions.
     * @param mitigationName MitigationName constraint for the active mitigations.
     * @param tsdMetrics 
     * @return List of MitigationRequestDescription instances, each instance describes an active request.
     */
    @Override
    public List<MitigationRequestDescription> getMitigationRequestDescriptionsForMitigation(@Nonnull String serviceName, @Nonnull String deviceName, String deviceScope, 
                                                                                            @Nonnull String mitigationName, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(deviceScope);
        Validate.notEmpty(mitigationName);
        Validate.notNull(tsdMetrics);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDescriptionsForMitigation");
        try {
            String tableName = mitigationRequestsTableName;
            String indexToUse = MitigationRequestsModel.MITIGATION_NAME_LSI;
            
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = new HashMap<>();
            Map<String, Condition> queryFilter = new HashMap<>();
            
            AttributeValue value = new AttributeValue(deviceName);
            Condition deviceCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            keyConditions.put(DEVICE_NAME_KEY, deviceCondition);
            
            value = new AttributeValue(mitigationName);
            Condition mitigationNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            keyConditions.put(MITIGATION_NAME_KEY, mitigationNameCondition);
            
            // Generate query filters to use when querying.
            
            // Restrict results to this serviceName
            value = new AttributeValue(serviceName);
            Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            queryFilter.put(SERVICE_NAME_KEY, serviceNameCondition);
            
            // Restrict results to this deviceName
            value = new AttributeValue(deviceScope);
            Condition deviceScopeCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            queryFilter.put(DEVICE_SCOPE_KEY, deviceScopeCondition);
            
            // Restrict results to only requests which are "active" (i.e. there hasn't been a subsequent workflow updating the actions of this workflow)
            value = new AttributeValue().withN("0");
            Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            queryFilter.put(UPDATE_WORKFLOW_ID_KEY, condition);
            
            // Ignore the delete requests, since they don't contain any mitigation definitions.
            value = new AttributeValue(RequestType.DeleteRequest.name());
            Condition requestTypeCondition = new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(value);
            queryFilter.put(REQUEST_TYPE_KEY, requestTypeCondition);            
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            Set<String> attributes = Sets.newHashSet(MitigationRequestsModel.getAttributeNamesForRequestTable());
            
            QueryRequest queryRequest = generateQueryRequest(attributes, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
            
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
            
            // Convert the fetched items into a list of ActiveMitigationDetails.
            return getMitigationDescriptions(fetchedItems);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper method to convert a list of items (Map of AttributeName (String) to AttributeValue) fetched from DDB into a List of MitigationRequestDescription instances.
     * @param items List of Map, where each Map has attributeName (String) as the key and DDB's AttributeValue as the corresponding value.
     * @return List of MitigationRequestDescription instances, each instance describes a request.
     */
    private List<MitigationRequestDescription> getMitigationDescriptions(@Nonnull List<Map<String, AttributeValue>> items) {
        List<MitigationRequestDescription> descriptions = new ArrayList<>();
        
        for (Map<String, AttributeValue> item : items) {
            MitigationRequestDescription description = convertToRequestDescription(item);
            descriptions.add(description);
        }
        
        return descriptions;
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
