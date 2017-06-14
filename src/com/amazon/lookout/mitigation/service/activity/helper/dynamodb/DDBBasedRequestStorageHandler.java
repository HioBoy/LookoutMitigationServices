package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBRequestSerializer.RequestSummary;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

/**
 * DDBBasedRequestStorageHandler is an abstract class meant to be a helper for concrete request storage handler implementations.
 * The requests are stored in the MITIGATION_REQUESTS table, where we have a separate table per domain (eg: MITIGATION_REQUESTS_{BETA/GAMMA/PROD}).
 * Details of this table can be found here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#MITIGATION_REQUESTS
 * 
 */
public class DDBBasedRequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedRequestStorageHandler.class);
    
    // Log prefix to use if the sanity check for workflowId fails - we can use this tag to set logscan alarms on.
    private static final String WORKFLOW_ID_SANITY_CHECK_FAILURE_LOG_PREFIX = "[WORKFLOW_ID_RANGE_CHECK_FAILURE] ";

    public static final String DDB_QUERY_FAILURE_COUNT = "DynamoDBQueryFailureCount";
    
    private static final int NUM_RECORDS_TO_FETCH_FOR_MAX_WORKFLOW_ID = 5;

    // Below is a list of relevant keys from the Active Mitigations table.
    public static final String DELETION_DATE_KEY = "DeletionDate";
    
    // Keys for TSDMetrics.
    public static final String NUM_ACTIVE_MITIGATIONS_FOR_DEVICE = "NumActiveMitigations";
    public static final String NUM_DDB_PUT_ITEM_ATTEMPTS_KEY = "NumPutAttempts";
    public static final String NUM_DDB_QUERY_ATTEMPTS_KEY = "NumDDBQueryAttempts";
    public static final String NUM_DDB_UPDATE_ITEM_ATTEMPTS_KEY = "NumDDBUpdateAttempts";
    public static final String NUM_DDB_GET_ITEM_ATTEMPTS_KEY = "NumDDBGetAttempts";
    public static final String FAILED_TO_FETCH_LATEST_VERSION_KEY = "FailedToFetchLatestVersion";
    public static final String DDB_QUERY_ERROR = "NumDDBQueryErrors"; // Number of failures that we retried
    public static final String DDB_QUERY_FAULT_COUNT = "NumDDBQueryFaults"; // Number of failures that we gave up on
    
    protected static final String INVALID_REQUEST_TO_STORE_KEY = "InvalidRequestToStore";
    
    // Retry and sleep configs.
    protected static final int DDB_ACTIVITY_MAX_ATTEMPTS = 10;
    
    public static final int DDB_QUERY_MAX_ATTEMPTS = 3;
    private static final long DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    protected static final int DDB_PUT_ITEM_MAX_ATTEMPTS = 3;
    private static final long DDB_PUT_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Slightly higher number of retries for updating item, since we've already done a whole lot of work for creating the item.
    protected static final int DDB_UPDATE_ITEM_MAX_ATTEMPTS = 5;
    private static final long DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    public static final int INITIAL_MITIGATION_VERSION = 1;
    protected static final String UNEDITED_MITIGATIONS_LSI_NAME = MitigationRequestsModel.UNEDITED_MITIGATIONS_LSI_NAME;
    
    private static final RecursiveToStringStyle recursiveToStringStyle = new RecursiveToStringStyle();
    
    private final AmazonDynamoDB dynamoDBClient;
    private final DynamoDB dynamoDB;
    private final String mitigationRequestsTableName;
    private final Table mitigationRequestsTable;
    
    public DDBBasedRequestStorageHandler(@NonNull AmazonDynamoDB dynamoDBClient, @NonNull String domain) {
        this.dynamoDBClient = dynamoDBClient;
        this.dynamoDB = new DynamoDB(dynamoDBClient);
        
        Validate.notEmpty(domain);
        this.mitigationRequestsTableName = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.mitigationRequestsTable = dynamoDB.getTable(mitigationRequestsTableName);
    }
    
    protected Table getMitigationRequestsTable() {
        return mitigationRequestsTable;
    }
    
    protected String getMitigationRequestsTableName() {
        return mitigationRequestsTableName;
    }
    
    /**
     * Helper method to store a request into DDB.
     * @param request Request to be stored.
     * @param mitigationDefinition : mitigation definition
     * @param locations Set of String representing the locations where this request applies.
     * @param deviceNameAndScope DeviceNameAndScope for this request.
     * @param workflowId WorkflowId to use for storing this request.
     * @param requestType Indicates the type of request made to the service.
     * @param mitigationVersion Version number to be associated with this mitigation to be stored.
     * @param metrics
     * @throws ConditionalCheckFailedException : when conditional check failed in put item.
     */
    protected void storeRequestInDDB(MitigationModificationRequest request, MitigationDefinition mitigationDefinition,
            Set<String> locations, DeviceNameAndScope deviceNameAndScope, long workflowId, 
            RequestType requestType, int mitigationVersion, TSDMetrics metrics) throws ConditionalCheckFailedException 
    {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.storeRequestInDDB")) {
            Map<String, AttributeValue> attributeValuesInItem = 
                    DDBRequestSerializer.serializeRequest(
                            request, mitigationDefinition, locations, deviceNameAndScope, workflowId, requestType, mitigationVersion,
                            MitigationRequestsModel.UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS);
            Map<String, ExpectedAttributeValue> expectedConditions = DDBRequestSerializer.expectedConditionsForNewRecord();
            
            int numAttempts = 0;
            // Try for a fixed number of times to store the item into DDB. If we succeed, we simply return, else we exit the loop and throw back an exception.
            while (true) {
                numAttempts++;
                subMetrics.addOne(NUM_DDB_PUT_ITEM_ATTEMPTS_KEY);
                try {
                    putItemInDDB(attributeValuesInItem, expectedConditions, subMetrics);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    throw ex;
                } catch (AmazonClientException ex) {
                    if (numAttempts < DDB_PUT_ITEM_MAX_ATTEMPTS) {
                        String msg = "Caught exception when inserting item: " + attributeValuesInItem + " into table: " + mitigationRequestsTableName + 
                                ". Num attempts so far: " + numAttempts + " " + ReflectionToStringBuilder.toString(request) + " for locations: " + locations;
                        LOG.warn(msg, ex);
                        sleepForPutRetry(numAttempts);
                    } else {
                        String msg = "Unable to insert item: " + attributeValuesInItem + " into table: " + mitigationRequestsTableName + " after " + numAttempts + 
                                "attempts for request: " + ReflectionToStringBuilder.toString(request) + " for locations: " + locations;
                        LOG.warn(msg);
                        throw ex;
                    }
                }
            }
        }
    }

    /**
     * Stores the attributeValues into the DDBTable.
     * Protected for unit tests.
     * @param attributeValues AttributeValues to store.
     * @param metrics
     */
    protected void putItemInDDB(Map<String, AttributeValue> attributeValues, Map<String, ExpectedAttributeValue> expectedConditions, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.putItemInDDB");
        try {
            PutItemRequest putItemRequest = new PutItemRequest(mitigationRequestsTableName, attributeValues);
            putItemRequest.setExpected(expectedConditions);
            dynamoDBClient.putItem(putItemRequest);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Called by the concrete implementations of this StorageHandler to find all the currently active mitigations for a device.
     * @param deviceName Device corresponding to whom all active mitigations need to be determined.
     * @param deviceScope Device scope for the device where all the active mitigations need to be determined.
     * @param lastEvaluatedKey Last evaluated key, to handle paginated response.
     * @param metrics
     * @return Long representing the max of the workflowId from the workflows that currently exist in the DDB tables.
     */
    protected Long getMaxWorkflowIdForDevice(String deviceName, String deviceScope, Long maxWorkflowIdOnLastAttempt, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.getMaxWorkflowIdForDevice");
        try {
            Map<String, Condition> keyConditions = 
                    DDBRequestSerializer.getPrimaryQueryKey(deviceName, maxWorkflowIdOnLastAttempt);
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest request = new QueryRequest();
                request.setAttributesToGet(Collections.singleton(MitigationRequestsModel.WORKFLOW_ID_KEY));
                request.setTableName(mitigationRequestsTableName);
                request.setConsistentRead(true);
                request.setKeyConditions(keyConditions);
                if (lastEvaluatedKey != null) {
                    request.setExclusiveStartKey(lastEvaluatedKey);
                }
                
                // Set the scan index forward to false - to allow querying the index backwards. When getting the maxWorkflowId we always query by the primary key (DeviceName+WorkflowId)
                // thus having the results sent to us in the reverse order will ensure that the first record we get is the max of the current set of workflowIds.
                request.setScanIndexForward(false);
                request.setLimit(NUM_RECORDS_TO_FETCH_FOR_MAX_WORKFLOW_ID);
                
                // Filter out any records whose DeviceScope isn't the same.
                AttributeValue deviceScopeAttrVal = new AttributeValue(deviceScope);
                Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(Arrays.asList(deviceScopeAttrVal));
                request.addQueryFilterEntry(MitigationRequestsModel.DEVICE_SCOPE_KEY, condition);
                
                QueryResult queryResult = queryDynamoDBWithRetries(request, subMetrics);
                
                lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                if (queryResult.getCount() > 0) {
                    // Since we query the primary key and set the index to be iterated in the reverse order, the first result we get back is the max workflowId.
                    return Long.parseLong(queryResult.getItems().get(0).get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN());
                }
            } while (lastEvaluatedKey != null);
            
            return null;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Get the latest request summary for given mitigation on the given device and scope
     * @param deviceName Device corresponding to whom the latest mitigation version needs to be determined.
     * @param deviceScope Device scope corresponding to whom the latest mitigation version needs to be determined.
     * @param mitigationName Mitigation Name corresponding to whom the latest mitigation version needs to be determined.
     * @param latestFromLastAttempt latest version and type from last attempt, could be null.
     * @param metrics
     * @return RequestSummary representing the latest mitigation version that currently exist in the DDB tables,
     *      if mitigation does not exist, return null.
     */
    protected RequestSummary getLatestRequestSummary(
            @NonNull String deviceName, @NonNull String deviceScope, @NonNull String mitigationName, 
            RequestSummary latestFromLastAttempt, @NonNull TSDMetrics metrics) 
    {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.getLatestVersionForMitigationOnDevice")) {
            subMetrics.addZero(FAILED_TO_FETCH_LATEST_VERSION_KEY);
            
            Map<String, Condition> keyConditions = DDBRequestSerializer.getQueryKeyForMitigationName(
                    deviceName, mitigationName);
            
            Map<String, Condition> queryFilter = new HashMap<>();
            DDBRequestSerializer.addDeviceScopeCondition(queryFilter, deviceScope);
            DDBRequestSerializer.addNotFailedCondition(queryFilter);
            if (latestFromLastAttempt != null) {
                DDBRequestSerializer.addMinVersionCondition(queryFilter, latestFromLastAttempt.getMitigationVersion());
            }
            
            QueryRequest request = new QueryRequest().withAttributesToGet(DDBRequestSerializer.RequestSummary.getRequiredAttributes())
                                                     .withTableName(mitigationRequestsTableName)
                                                     .withIndexName(MitigationRequestsModel.MITIGATION_NAME_LSI)
                                                     .withConsistentRead(true)
                                                     .withKeyConditions(keyConditions)
                                                     .withQueryFilter(queryFilter);
            
            RequestSummary latestSummary = latestFromLastAttempt;
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                if (lastEvaluatedKey != null) {
                    request.setExclusiveStartKey(lastEvaluatedKey);
                }
                
                QueryResult queryResult = queryDynamoDBWithRetries(request, subMetrics);
                
                lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                for (Map<String, AttributeValue> item : queryResult.getItems()) {
                    RequestSummary summary = new RequestSummary(item);
                    if ((latestSummary == null) || 
                        (summary.getWorkflowId() > latestSummary.getWorkflowId())) 
                    {
                        latestSummary = summary;
                    }
                }
            } while (lastEvaluatedKey != null);
            
            return latestSummary;
        }
    }
    
    /**
     * Called by the concrete implementations of this StorageHandler to find all the currently active mitigations for a device.
     * @param deviceName Device corresponding to whom all active mitigations need to be determined.
     * @param deviceScope Device scope for the device where all the active mitigations need to be determined.
     * @param attributesToGet Set of attributes to retrieve for each active mitigation.
     * @param keyConditions Map of String (attributeName) and Condition - Condition represents constraint on the attribute. Eg: >= 5.
     * @param lastEvaluatedKey Last evaluated key, to handle paginated response.
     * @param indexToUse Specifies the index to use for issuing the query against DDB. Null implies we will query the primary key.
     * @param queryFilters Map of String (attributeName) and Condition - Condition represents a constraint on the attribute to filter the results by.
     * @param metrics
     * @return QueryResult representing the result from issuing this query to DDB.
     */
    protected QueryResult getActiveMitigationsForDevice(String deviceName, String deviceScope, Set<String> attributesToGet, Map<String, Condition> keyConditions, 
                                                        Map<String, AttributeValue> lastEvaluatedKey, String indexToUse, Map<String, Condition> queryFilters, TSDMetrics metrics) {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.getActiveMitigationsForDevice")) {
            QueryRequest request = new QueryRequest();
            request.setTableName(mitigationRequestsTableName);
            request.setConsistentRead(true);
            request.setKeyConditions(keyConditions);
            
            request.setAttributesToGet(attributesToGet);
            request.withQueryFilter(queryFilters);
            request.setExclusiveStartKey(lastEvaluatedKey);
            request.setIndexName(indexToUse);
            
            // Filter out any records whose DeviceScope isn't the same.
            if ((request.getQueryFilter() == null) || !request.getQueryFilter().containsKey(MitigationRequestsModel.DEVICE_SCOPE_KEY)) {
                AttributeValue deviceScopeAttrVal = new AttributeValue(deviceScope);
                Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(Arrays.asList(deviceScopeAttrVal));
                request.addQueryFilterEntry(MitigationRequestsModel.DEVICE_SCOPE_KEY, condition);
            }
        
            return queryDynamoDBWithRetries(request, subMetrics);
        }
    }

    /**
     * Responsible for recording the SWFRunId corresponding to the workflow request just created in DDB.
     * 
     * Used by reaper to match workflow run with same workflow ID
     * 
     * @param deviceName DeviceName corresponding to the workflow being run.
     * @param workflowId WorkflowId for the workflow being run.
     * @param runId SWF assigned runId for the running instance of this workflow.
     * @param metrics
     */
    public void updateRunIdForWorkflowRequest(@NonNull String deviceName, long workflowId, @NonNull String runId, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(workflowId > 0);
        Validate.notEmpty(runId);
        Validate.notNull(metrics);
        
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.updateRunIdForWorkflowRequest")) {
            Map<String, AttributeValue> key = DDBRequestSerializer.getKey(deviceName, workflowId);
            
            Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
            attributeUpdates.put(MitigationRequestsModel.SWF_RUN_ID_KEY, new AttributeValueUpdate(new AttributeValue(runId), AttributeAction.PUT));

            Map<String, ExpectedAttributeValue> expected = new HashMap<>();
            ExpectedAttributeValue absentValueExpectation = new ExpectedAttributeValue();
            absentValueExpectation.setExists(false);
            expected.put(MitigationRequestsModel.SWF_RUN_ID_KEY, absentValueExpectation);
            
            // Attempt to update DDB for a fixed number of times.
            int numAttempts = 0;
            while (true) {
                subMetrics.addOne(NUM_DDB_UPDATE_ITEM_ATTEMPTS_KEY);
                try {
                    updateItemInDynamoDB(attributeUpdates, key, expected, metrics);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    String msg = "For workflowId: " + workflowId + " for device: " + deviceName + " attempted runId update: " + runId + 
                                 ", but caught a ConditionalCheckFailedException indicating runId was already updated!";
                    LOG.warn(msg, ex);
                    // Just return - what most likely happened is that this is a retry and the previous call timedout 
                    // but actually completed in the background so we're now seeing our own update
                    return;
                } catch (AmazonClientException ex) {
                    if (numAttempts < DDB_UPDATE_ITEM_MAX_ATTEMPTS) {
                        String msg = "Caught Exception when updating runId to :" + runId + " for device: " + deviceName + 
                                     " for workflowId: " + workflowId + ". Attempts so far: " + numAttempts;
                        LOG.warn(msg, ex);
        
                        sleepForUpdateRetry(numAttempts);
                    } else {
                        String msg = "Unable to update runId to :" + runId + " for device: " + deviceName + 
                                " for workflowId: " + workflowId + " after " + numAttempts + " number of attempts.";
                        LOG.warn(msg, ex);
                        throw ex;
                    }
                }
            }
         }
    }

    /**
     * Responsible for updating the abortflag of the request in DDB.
     * 
     * Used by the abort deployment activity. 
     * 
     * @param deviceName DeviceName corresponding to the workflow being run.
     * @param workflowId WorkflowId for the workflow being run.
     * @param abortFlag true if we want to abort the workflow, false otherwise.
     * @param metrics
     */
    public void updateAbortFlagForWorkflowRequest(@NonNull String deviceName, long workflowId, boolean abortFlag , @NonNull TSDMetrics metrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(workflowId > 0);
        Validate.notNull(metrics);
        
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.updateAbortFlagForWorkflowRequest")) {
            Map<String, AttributeValue> key = DDBRequestSerializer.getKey(deviceName, workflowId);
            
            Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
            attributeUpdates.put(MitigationRequestsModel.ABORT_FLAG_KEY, new AttributeValueUpdate(new AttributeValue().withBOOL(abortFlag), AttributeAction.PUT));

            Map<String, ExpectedAttributeValue> expected = new HashMap<>();
            
            // Attempt to update DDB for a fixed number of times.
            int numAttempts = 0;
            while (true) {
                subMetrics.addOne(NUM_DDB_UPDATE_ITEM_ATTEMPTS_KEY);
                try {
                    updateItemInDynamoDB(attributeUpdates, key, expected, metrics);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    String msg = "For workflowId: " + workflowId + "  device: " + deviceName + " attempted update abortFlag to: " + abortFlag + 
                                 ", but caught a ConditionalCheckFailed exception";
                    LOG.error(msg, ex);
                    throw ex;
                } catch (AmazonClientException ex) {
                    if (numAttempts < DDB_UPDATE_ITEM_MAX_ATTEMPTS) {
                        String msg = "Caught Exception when updating abortFlag to :" + abortFlag + " for device: " + deviceName + 
                                     " workflowId: " + workflowId + ". # of Attempts so far: " + numAttempts;
                        LOG.warn(msg, ex);
        
                        sleepForUpdateRetry(numAttempts);
                    } else {
                        String msg = "Failed to update abortFlag to :" + abortFlag + " for device: " + deviceName + 
                                " workflowId: " + workflowId + " after " + numAttempts + " number of attempts.";
                        LOG.warn(msg, ex);
                        throw ex;
                    }
                }
            }
         }
    }
    /* Called by the concrete implementations of this StorageHandler to query DDB.
     * @param request QueryRequest keys containing the information to use for querying.
     * @param metrics
     * @return GetItemResult representing the result from issuing this query to DDB.
     */
    protected GetItemResult getRequestInDDB(Map<String, AttributeValue> keys, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.getRequestInDDB");
        int numAttempts = 0;
        try {
            // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop endsthrow back an exception.
            while (true) {
                try {
                    return getItemInDynamoDB(keys, subMetrics);
                } catch (AmazonClientException ex) {
                    if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                        String msg = "Caught Exception when trying to query DynamoDB. Attempts so far: " + numAttempts;
                        LOG.warn(msg, ex);
                        sleepForQueryRetry(numAttempts);
                    } else {
                        String msg = "Unable to get the item from DDB. Total NumAttempts: " + numAttempts;
                        LOG.warn(msg, ex);
                        throw ex;
                    }
                }
            }
        } finally {
            subMetrics.addCount(NUM_DDB_GET_ITEM_ATTEMPTS_KEY, numAttempts);
            subMetrics.end();
        }
    }
    
    
    /**
     * Called by the concrete implementations of this StorageHandler to query DDB.
     * @param request QueryRequest object containing the information to use for querying.
     * @param metrics
     * @return QueryResult representing the result from issuing this query to DDB.
     */
    protected QueryResult queryDynamoDBWithRetries(QueryRequest request, TSDMetrics metrics) {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.queryDynamoDBWithRetries")) {
            // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop endsthrow back an exception.
            int numAttempts = 0;
            while (true) {
                numAttempts++;
                subMetrics.addOne(NUM_DDB_QUERY_ATTEMPTS_KEY);
                
                try {
                    return queryDynamoDB(request, metrics);
                } catch (AmazonClientException ex) {
                    metrics.addOne(DDB_QUERY_FAILURE_COUNT);
                    if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                        LOG.warn("Caught exception querying DynamoDB. Attempts so far: " + numAttempts, ex);
                        sleepForQueryRetry(numAttempts);
                    } else {
                        metrics.addOne(DDB_QUERY_FAULT_COUNT);
                        LOG.error("Failed querying DynamoDB after " + numAttempts + " attempts.", ex);
                        throw ex;
                    }
                }
            }
        }
    }
    
    /**
     * Issues a query against the DDBTable. 
     * Protected for unit tests.
     * @param request Request to be queried.
     * @param metrics
     * @return QueryResult corresponding to the result of the query.
     */
    private QueryResult queryDynamoDB(QueryRequest request, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.queryDynamoDB");
        try {
            return dynamoDBClient.query(request);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Issues a get operation against the DDBTable. 
     * @param keys Primary key attribute values to search for.
     * @param metrics
     * @return GetItemResult corresponding to the result of the get request.
     */
    private GetItemResult getItemInDynamoDB(Map<String, AttributeValue> keys, TSDMetrics metrics) {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.queryDynamoDB")) {
            GetItemResult result = dynamoDBClient.getItem(
                    new GetItemRequest().withTableName(mitigationRequestsTableName).withKey(keys));
            return result;
        }
    }
    
    /**
     * Update an item
     */
    private void updateItemInDynamoDB(
            Map<String, AttributeValueUpdate> attributeUpdates, Map<String, AttributeValue> key, Map<String, ExpectedAttributeValue> expected,
            TSDMetrics metrics) 
    {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.updateItemInDynamoDB")) {
            UpdateItemRequest request = new UpdateItemRequest()
                .withTableName(mitigationRequestsTableName)
                .withAttributeUpdates(attributeUpdates)
                .withKey(key)
                .withExpected(expected);
            
            dynamoDBClient.updateItem(request);
        }
    }
    
    /**
     * Check if the new workflowId we're going to use is within the valid range for this deviceScope.
     * @param workflowId
     * @param deviceNameAndScope
     */
    protected void sanityCheckWorkflowId(long workflowId, DeviceNameAndScope deviceNameAndScope) {
        DeviceScope deviceScope = deviceNameAndScope.getDeviceScope();
        if ((workflowId < deviceScope.getMinWorkflowId()) || (workflowId > deviceScope.getMaxWorkflowId())) {
            String msg = WORKFLOW_ID_SANITY_CHECK_FAILURE_LOG_PREFIX + "Received workflowId = " + workflowId + " which is out of the valid range for the device scope: " + 
                         deviceScope.name() + " expectedMin: " + deviceScope.getMinWorkflowId() + " expectedMax: " + deviceScope.getMaxWorkflowId();
            LOG.warn(msg);
            throw new InternalServerError500(msg);
        }
    }
    
    protected void checkMitigationVersion(
            MitigationModificationRequest request, DeviceNameAndScope deviceNameAndScope,
            int newMitigationVersion, int latestVersion) 
    {
        String mitigationName = request.getMitigationName();
        String deviceName = deviceNameAndScope.getDeviceName().name();
        String deviceScope = deviceNameAndScope.getDeviceScope().name();
        
        if (newMitigationVersion <= latestVersion) {
            String msg = "Mitigation found in DDB for deviceName: " + deviceName
                    + " and deviceScope: " + deviceScope + " and mitigationName: "
                    + mitigationName + " has a newer version: " + latestVersion
                    + " than one in request: " + newMitigationVersion + ". For request: "
                    + requestToString(request);
            LOG.info(msg);
            throw new StaleRequestException400(msg);
        }

        final int expectedMitigationVersion = latestVersion + 1;
        if (newMitigationVersion != expectedMitigationVersion) {
            String msg = "Unexpected mitigation version in request for deviceName: " + deviceName
                    + " and deviceScope: " + deviceScope + " and mitigationName: " + mitigationName
                    + " Expected mitigation version: " + expectedMitigationVersion
                    + " , but mitigation version in request was: " + newMitigationVersion
                    + ". Request: " + requestToString(request);
            LOG.info(msg);
            // TODO Fix this to be more specific
            throw new IllegalArgumentException(msg);
        }
    }

    protected static String requestToString(MitigationModificationRequest request) {
        return ReflectionToStringBuilder.toString(request, recursiveToStringStyle);
    }
    
    protected void sleepForPutRetry(int attemptNumber) {
        try {
            Thread.sleep(DDB_PUT_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER * attemptNumber);
        } catch (InterruptedException ignored) {
            throw new AbortedException("Interrupted while sleeping for a put retry");
        }
    }
    
    protected void sleepForQueryRetry(int attemptNumber) {
        try {
            Thread.sleep(DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER * attemptNumber);
        } catch (InterruptedException ignored) {
            throw new AbortedException("Interrupted while sleeping for a query retry");
        }
    }
    
    protected void sleepForUpdateRetry(int attemptNumber) {
       try {
           Thread.sleep(DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER * attemptNumber);
       } catch (InterruptedException ignored) {
           throw new AbortedException("Interrupted while sleeping for a update retry");
       }
    }
}
