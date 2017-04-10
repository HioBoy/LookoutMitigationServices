package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.ddb.model.ActiveMitigationsModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.activities.model.MitigationInstanceSchedulingStatus;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.status.helper.ActiveMitigationsStatusHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.google.common.collect.Sets;

public class DDBBasedListMitigationsHandler extends DDBBasedRequestStorageHandler implements RequestInfoHandler, ActiveMitigationInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedListMitigationsHandler.class);
    
    public static final String ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX = "ACTIVE_MITIGATIONS_";
    
    // Num Attempts + Retry Sleep Configs.
    public static final int DDB_ACTIVITY_MAX_ATTEMPTS = 3;
    private static final long DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Keys for TSDMetric properties.
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_SUMMARY = "NumGetMitigationDescriptionAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_NAME_AND_REQUEST_STATUS = "NumGetMitigationNameAndRequestStatusAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE = "NumGetActiveMitigationsForServiceAttempts";
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_REQUEST_INFO = "NumGetMitigationRequestInfo";

    private static final Integer MITIGATION_HISTORY_EVALUCATE_ITEMS_COUNT_PER_QUERY_BASE = 25;
    
    private final DDBBasedMitigationInstanceHandler mitigationInstanceHandler;    
    private final ActiveMitigationsStatusHelper activeMitigationStatusHelper;
    private final String activeMitigationsTableName;

    public DDBBasedListMitigationsHandler(AmazonDynamoDB dynamoDBClient, String domain, @NonNull ActiveMitigationsStatusHelper activeMitigationStatusHelper) {
        super(dynamoDBClient, domain);
        this.activeMitigationStatusHelper = activeMitigationStatusHelper;
        
        this.activeMitigationsTableName = ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.mitigationInstanceHandler = new DDBBasedMitigationInstanceHandler(dynamoDBClient, domain);
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
            Map<String, AttributeValue> key = DDBRequestSerializer.getKey(deviceName, jobId);
            GetItemResult result = getRequestInDDB(key, subMetrics);
            Map<String, AttributeValue> item = result.getItem();
            if (CollectionUtils.isEmpty(item)) {
                return null;
            }
            return DDBRequestSerializer.convertToRequestDescription(item);
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the mitigation request associated with the device: " + deviceName + " and jobId: " + jobId;
            LOG.warn(msg, ex);
            throw ex;
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
            MitigationRequestDescription description = 
                    getMitigationRequestDescription(deviceName, jobId, tsdMetrics);
            
            if (description == null) {
                String msg = String.format("Could not find an item for the requested jobId %d, device name %s, and template %s", jobId, deviceName, templateName);
                LOG.info(msg);
                throw new IllegalStateException(msg);
            }

            String resultTemplateName = description.getMitigationTemplate();
            if (!templateName.equals(resultTemplateName)) {
                String msg = String.format("The request associated with job id: %s is associated with a different template than requested: %s", jobId, templateName);
                LOG.info(msg + " Expected template: " + resultTemplateName);
                throw new IllegalStateException(msg);
            }
            
            String mitigationName = description.getMitigationName();
            String requestStatus = description.getRequestStatus();
            
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
            String indexToUse = ActiveMitigationsModel.DEVICE_NAME_LSI;
            
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
                        QueryResult result = queryDynamoDBWithRetries(queryRequest, subMetrics);
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
                    
                    sleepForActivityRetry(numAttempts);
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
     * Searches backwards through mitigation requests to get all running request of each location (whose WorkflowStatus is RUNNING)
     * @param serviceName Service for which we need to fetch the mitigation request description for ongoing requests.
     * @param deviceName DeviceName for which we need to fetch the mitigation request description for ongoing requests.
     * @param locations locations for which we need to fetch the mitigation request description for ongoing requests.(can be null)
     * @param tsdMetrics
     * @return a List of MitigationRequestDescription, where each MitigationRequestDescription instance describes a mitigation that is currently being worked on (whose WorkflowStatus is RUNNING).
     */
    @Override
    public List<MitigationRequestDescriptionWithLocations> getOngoingRequestsDescription(@NonNull String serviceName, @NonNull String deviceName, List<String> locations, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getInProgressRequestsDescription");
        try {

            Map<String, MitigationRequestDescriptionWithLocations> remainingLocations = null;
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = DDBRequestSerializer.getQueryKeyForDevice(deviceName);
            
            // Generate query filters to use when querying.
            Map<String, Condition> queryFilter = new HashMap<>();
            Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                            .withAttributeValueList(new AttributeValue(serviceName));
            queryFilter.put(MitigationRequestsModel.SERVICE_NAME_KEY, serviceNameCondition);
            QueryRequest queryRequest = new QueryRequest().withTableName(getMitigationRequestsTableName())
                    .withKeyConditions(keyConditions)
                    .withQueryFilter(queryFilter)
                    .withConsistentRead(true)
                    .withScanIndexForward(false); 
            
            if (!CollectionUtils.isEmpty(locations)) {
                //with location filter
                remainingLocations = new HashMap<>();
                for (String location: locations) {
                    remainingLocations.put(location, null);
                }               
            } else {
                //without location filter, search only(and all) running requests with updateworkflowid=0;
                DDBRequestSerializer.addActiveRequestCondition(keyConditions);
                Condition runningWorkflowCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                                    .withAttributeValueList(new AttributeValue(WorkflowStatus.RUNNING));
                queryFilter.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, runningWorkflowCondition);
                queryRequest.setKeyConditions(keyConditions);
                queryRequest.setQueryFilter(queryFilter);
                queryRequest.setIndexName(UNEDITED_MITIGATIONS_LSI_NAME);
            }

            Map<String, AttributeValue> lastEvaluatedKey = null;         
            List<MitigationRequestDescriptionWithLocations> descriptions = new ArrayList<>();
            // query the MITIGATION_REQUESTS table backwards, starting with the most recent WorkflowId.
            // three cases we need handle here:
            // 1. find a running request
            //      This is exactly the request we are looking for, simply save it. 
            // 2. find a succeeded request:
            //      This means all prior requests for locations in this request should have already completed;
            //      so it is safe to stop searching running requests for those locations now
            // 3. find a request in other status (FAILED or PARTIAL_SUCCESS or INDETERMINATE):
            //      Because request can fail out of order due to SWF workflow timeouts or failures,
            //      the only way to know there aren't any previous RUNNING requests is to inspect the MITIGATION_INSTANCE of each location for the failed request
            //      to check if the instance is completed due to running and failing instead of being reaped. 
            //      If the instance of a location has a non-blank BlockingWorkflowId attribute value, 
            //          then the instance completed while still blocked, so there may still be previous RUNNING requests for that location and we need keep searching backwards.
            //      otherwise, the instance completed and is not blocked, so there shouldn't be any running requests for that location and we can safely stop searching running request for this location. 
            do {
                int numAttempts = 0;
                Throwable lastCaughtException = null;
                while (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    ++numAttempts;
                    subMetrics.addOne(NUM_ATTEMPTS_TO_GET_ACTIVE_MITIGATIONS_FOR_SERVICE);
                    
                    try {
                        queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                        QueryResult result = queryDynamoDBWithRetries(queryRequest, subMetrics);
                        if (result.getCount() > 0) {
                            for (Map<String, AttributeValue> item : result.getItems()) {
                                MitigationRequestDescriptionWithLocations description = DDBRequestSerializer.convertToRequestDescriptionWithLocations(item);
                                String workflowStatus = description.getMitigationRequestDescription().getRequestStatus();
                                if (remainingLocations != null) {
                                    List<String> locationsInRequest = new ArrayList<>(description.getLocations());
                                    // filter requests by locations
                                    locationsInRequest.retainAll(remainingLocations.keySet());
                                    if (!locationsInRequest.isEmpty()) {
                                        if (workflowStatus.equals(WorkflowStatus.RUNNING)) {
                                            // 1. find a running request
                                            //      This is exactly the request we are looking for, simply save it. 
                                            descriptions.add(description);
                                        } else if (workflowStatus.equals(WorkflowStatus.SUCCEEDED)) {
                                            // 2. find a succeeded request:
                                            //      This means all prior requests for locations in this request should have already completed;
                                            //      so it is safe to stop searching running request for those locations now
                                            remainingLocations.keySet().removeAll(locationsInRequest);
                                            if (remainingLocations.isEmpty()) {
                                                break;
                                            }
                                        } else {
                                            // 3. find a request in other status (FAILED or PARTIAL_SUCCESS or INDETERMINATE):
                                            //      Because request can fail out of order due to SWF workflow timeouts or failures,
                                            //      the only way to know there aren't any previous RUNNING requests is to inspect the MITIGATION_INSTANCE of each location for the failed request
                                            //      to check if the instance is completed due to running and failing instead of being reaped. 
                                            for (String location : locationsInRequest) {
                                                if (remainingLocations.containsKey(location)) {
                                                    remainingLocations.put(location, description);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    if (workflowStatus.equals(WorkflowStatus.RUNNING)) {
                                        descriptions.add(description);
                                    }
                                }
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
                    
                    sleepForActivityRetry(numAttempts);
                }
                
                if (numAttempts >= DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = "Unable to query currently running mitigations associated with service: " + serviceName + " on device: " + deviceName +  
                                 ReflectionToStringBuilder.toString(queryRequest) + " after " + numAttempts + " attempts, Exception: " + lastCaughtException.toString();
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
                
                if (lastEvaluatedKey != null && remainingLocations != null) {
                    if (!remainingLocations.isEmpty()) {
                        List<String> locationsToRemove = new ArrayList<>();
                        //inspect the MITIGATION_INSTANCE of each location
                        //If the instance of a location has a non-blank BlockingWorkflowId attribute value, 
                        //then the instance completed while still blocked (request got reaped), so there may still be previous RUNNING requests for that location and we need keep searching backwards.
                        //otherwise, the instance completed and was not blocked, so there shouldn't be any running requests for that location and we can safely stop searching for this location now. 
                        remainingLocations.forEach((location, description) -> {
                            if (description != null) {
                                MitigationInstanceSchedulingStatus mitigationInstanceSchedulingStatus = mitigationInstanceHandler.getMitigationInstanceSchedulingStatus(
                                        description.getMitigationRequestDescription().getJobId(),
                                        description.getMitigationRequestDescription().getDeviceName(), location);
                                if (mitigationInstanceSchedulingStatus != null
                                        && mitigationInstanceSchedulingStatus.isFound()
                                        && !mitigationInstanceSchedulingStatus.isBlocked()
                                        && mitigationInstanceSchedulingStatus.isCompleted()) {
                                    locationsToRemove.add(location);
                                }
                            }
                        });
                        remainingLocations.keySet().removeAll(locationsToRemove);
                    }                
                
                    if (remainingLocations.isEmpty()) {
                        break;
                    }
                }

            } while (lastEvaluatedKey != null);
            
            return descriptions;
        } finally {
            subMetrics.end();
        }
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
    private static QueryRequest generateQueryRequest(Set<String> attributesToGet, Map<String, Condition> keyConditions,
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
     * @param listOfKeyValues A list of maps of Strings as keys and AttributeValues as keys.
     * @return a list of MitigationInstanceStatuses
     */
    private static List<ActiveMitigationDetails> activeMitigationsListConverter(List<Map<String, AttributeValue>> listOfKeyValues){ 
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
     * 
     * @param serviceName ServiceName for which we need to get the list of active mitigation descriptions.
     * @param deviceName DeviceName from which we need to read the list of active mitigation descriptions.
     * @param deviceScope DeviceScope to constraint the query to get the list of active mitigation descriptions.
     * @param mitigationName MitigationName constraint for the active mitigations.
     * @param tsdMetrics 
     * @return List of MitigationRequestDescription instances, should contain a single request representing the latest (create/edit) mitigation request for this mitigationName.
     */
    @Override
    public List<MitigationRequestDescription> getMitigationRequestDescriptionsForMitigation(@NonNull String serviceName, @NonNull String deviceName, String deviceScope,
                                                                                            @NonNull String mitigationName, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(deviceScope);
        Validate.notEmpty(mitigationName);
        
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDescriptionsForMitigation");
        try {
            String tableName = getMitigationRequestsTableName();
            String indexToUse = MitigationRequestsModel.MITIGATION_NAME_LSI;
            
            // Generate key condition to use when querying.
            Map<String, Condition> keyConditions = new HashMap<>();
            Map<String, Condition> queryFilter = new HashMap<>();
            
            AttributeValue value = new AttributeValue(deviceName);
            Condition deviceCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            keyConditions.put(MitigationRequestsModel.DEVICE_NAME_KEY, deviceCondition);
            
            value = new AttributeValue(mitigationName);
            Condition mitigationNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            keyConditions.put(MitigationRequestsModel.MITIGATION_NAME_KEY, mitigationNameCondition);
            
            // Generate query filters to use when querying.
            
            // Restrict results to this serviceName
            value = new AttributeValue(serviceName);
            Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            queryFilter.put(MitigationRequestsModel.SERVICE_NAME_KEY, serviceNameCondition);
            
            // Restrict results to this deviceName
            value = new AttributeValue(deviceScope);
            Condition deviceScopeCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
            queryFilter.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, deviceScopeCondition);
            
            // Ignore the delete requests, since they don't contain any mitigation definitions.
            value = new AttributeValue(RequestType.DeleteRequest.name());
            Condition requestTypeCondition = new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(value);
            queryFilter.put(MitigationRequestsModel.REQUEST_TYPE_KEY, requestTypeCondition);            
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            QueryRequest queryRequest = generateQueryRequest(null, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
            
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
                        QueryResult result = queryDynamoDBWithRetries(queryRequest, subMetrics);
                        fetchedItems.addAll(result.getItems());
                        lastEvaluatedKey = result.getLastEvaluatedKey();
                        break;
                    } catch (Exception ex) {
                        lastCaughtException = ex;
                        String msg = "Caught exception when querying mitigation request info associated with service: " + serviceName + " on device: " + deviceName + 
                                     ", ddbQuery: " + ReflectionToStringBuilder.toString(queryRequest);
                        LOG.warn(msg, ex);
                    }
                    
                    sleepForActivityRetry(numAttempts);
                }
                
                if (numAttempts >= DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = "Unable to query mitigation requests info associated with service: " + serviceName + " on device: " + deviceName +  
                                 ReflectionToStringBuilder.toString(queryRequest) + " after " + numAttempts + " attempts";
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
            } while (lastEvaluatedKey != null);
            
            List<MitigationRequestDescription> requestDescriptionsToReturn = new ArrayList<>();
            
            // Get the latest MitigationRequestDescription from the list of fetchedItems.
            if (!fetchedItems.isEmpty()) {
                MitigationRequestDescription requestDescription = getLatestMitigationDescription(fetchedItems);
                if (requestDescription != null) {
                    requestDescriptionsToReturn.add(requestDescription);
                }
            }
            
            return requestDescriptionsToReturn;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper method to convert a list of items (Map of AttributeName (String) to AttributeValue) fetched from DDB into MitigationRequestDescription instances,
     * returning back the latest MitigationRequestDescription, regardless of whether it is "active" or not (since it might have been deleted).
     * @param items List of Map, where each Map has attributeName (String) as the key and DDB's AttributeValue as the corresponding value.
     * @return MitigationRequestDescription representing the latest MitigationRequestDescription.
     */
    private static MitigationRequestDescription getLatestMitigationDescription(@NonNull List<Map<String, AttributeValue>> items) {
        MitigationRequestDescription latestRequestDescription = null;
        for (Map<String, AttributeValue> item : items) {
            MitigationRequestDescription description = DDBRequestSerializer.convertToRequestDescription(item);
            if (latestRequestDescription == null) {
                latestRequestDescription = description;
            } else {
                if (description.getJobId() > latestRequestDescription.getJobId()) {
                    latestRequestDescription = description;
                }
            }
        }
        
        return latestRequestDescription;
    }
    
    /**
     * Get mitigation definition based on device name, mitigation name, and mitigation version
     * This API queries GSI, result is eventually consistent.
     * @param deviceName : device name
     * @param serviceName: service name
     * @param mitigationName : mitigation name
     * @param mitigationVersion : mitigation version
     * @param tsdMetrics : TSD metrics
     * @return MitigationRequestDescription
     * @throws MissingMitigationVersionException404, if mitigation definition is not found
     */
    @Override
    public MitigationRequestDescriptionWithLocations getMitigationDefinition(String deviceName, String serviceName,
            String mitigationName, int mitigationVersion, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.notEmpty(mitigationName);
        Validate.isTrue(mitigationVersion > 0);

        QuerySpec query = new QuerySpec().withHashKey(MitigationRequestsModel.MITIGATION_NAME_KEY, mitigationName)
                .withRangeKeyCondition(new RangeKeyCondition(MitigationRequestsModel.MITIGATION_VERSION_KEY).eq(mitigationVersion))
                .withQueryFilters(new QueryFilter(MitigationRequestsModel.DEVICE_NAME_KEY).eq(deviceName),
                        new QueryFilter(MitigationRequestsModel.SERVICE_NAME_KEY).eq(serviceName),
                        new QueryFilter(MitigationRequestsModel.WORKFLOW_STATUS_KEY).ne(WorkflowStatus.FAILED))
                .withMaxResultSize(10);
        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedListMitigationsHandler.getMitigationDefinition")) {
            try {
                Index index = getMitigationRequestsTable().getIndex(MitigationRequestsModel.MITIGATION_NAME_MITIGATION_VERSION_GSI);
                for (Item item : index.query(query)) {
                    return DDBRequestSerializer.convertToRequestDescriptionWithLocations(item);
                }
            } catch (Exception ex) {
                String msg = "Unable to query mitigation requests info associated on device: " + deviceName
                        + ReflectionToStringBuilder.toString(query);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            throw new MissingMitigationVersionException404(String.format("Mitigation %s, version %d does not exist on device %s",
                    mitigationName, mitigationVersion, deviceName));
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
     */
    @Override
    public List<MitigationRequestDescriptionWithLocations> getMitigationHistoryForMitigation(
            String serviceName, String deviceName, String deviceScope,
            String mitigationName, Integer exclusiveStartVersion, 
            Integer maxNumberOfHistoryEntriesToFetch, TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(deviceScope);
        Validate.notEmpty(mitigationName);
        Validate.notNull(maxNumberOfHistoryEntriesToFetch);
        Validate.notNull(tsdMetrics);
        QuerySpec query = new QuerySpec().withHashKey(MitigationRequestsModel.MITIGATION_NAME_KEY, mitigationName)
                .withQueryFilters(new QueryFilter(MitigationRequestsModel.SERVICE_NAME_KEY).eq(serviceName),
                        new QueryFilter(MitigationRequestsModel.DEVICE_SCOPE_KEY).eq(deviceScope),
                        new QueryFilter(MitigationRequestsModel.DEVICE_NAME_KEY).eq(deviceName),
                        new QueryFilter(MitigationRequestsModel.WORKFLOW_STATUS_KEY).ne(WorkflowStatus.FAILED))
                .withMaxResultSize(maxNumberOfHistoryEntriesToFetch
                        // Max result size also decides how many items to evaluate in one query.
                        // To reduce the number of under-layer call to DDB service, we add a small overhead
                        // to accommodate the filtered items.
                        + MITIGATION_HISTORY_EVALUCATE_ITEMS_COUNT_PER_QUERY_BASE)
                .withScanIndexForward(false)
                .withConsistentRead(false);
                
        if (exclusiveStartVersion != null) {
            query.withRangeKeyCondition(new RangeKeyCondition(MitigationRequestsModel.MITIGATION_VERSION_KEY).lt(exclusiveStartVersion));
        }
        
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics(
                "DDBBasedListMitigationsHandler.getMitigationHistoryForMitigation")) {
                List<MitigationRequestDescriptionWithLocations> descs = new ArrayList<>(maxNumberOfHistoryEntriesToFetch);
            try {
                Index index = getMitigationRequestsTable().getIndex(MitigationRequestsModel.MITIGATION_NAME_MITIGATION_VERSION_GSI);
                for (Item item : index.query(query)) {
                    descs.add(DDBRequestSerializer.convertToRequestDescriptionWithLocations(item));
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
            return descs;
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
    
    protected void sleepForActivityRetry(int numAttempts) {
        if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
            try {
                Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
            } catch (InterruptedException ignored) {
                throw new AbortedException("Interrupted while sleeping for an activity retry");
            }
        }
    }
}
