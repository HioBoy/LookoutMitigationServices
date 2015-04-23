package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazon.aws158.commons.dynamo.DynamoDBHelper;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;

/**
 * DDBBasedRequestStorageHandler is an abstract class meant to be a helper for concrete request storage handler implementations.
 * The requests are stored in the MITIGATION_REQUESTS table, where we have a separate table per domain (eg: MITIGATION_REQUESTS_{BETA/GAMMA/PROD}).
 * Details of this table can be found here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#MITIGATION_REQUESTS
 * 
 */
public abstract class DDBBasedRequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedRequestStorageHandler.class);
    
    // Log prefix to use if the sanity check for workflowId fails - we can use this tag to set logscan alarms on.
    private static final String WORKFLOW_ID_SANITY_CHECK_FAILURE_LOG_PREFIX = "[WORKFLOW_ID_RANGE_CHECK_FAILURE] ";
    
    public static final String MITIGATION_REQUEST_TABLE_NAME_PREFIX = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX;
    public static final String ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX = "ACTIVE_MITIGATIONS_";
    
    private static final int NUM_RECORDS_TO_FETCH_FOR_MAX_WORKFLOW_ID = 5;
    
    // Below is a list of all the attributes we store in the MitigationRequests table.
    public static final String DEVICE_NAME_KEY = MitigationRequestsModel.DEVICE_NAME_KEY;
    public static final String WORKFLOW_ID_KEY = MitigationRequestsModel.WORKFLOW_ID_KEY;
    public static final String SWF_RUN_ID_KEY = MitigationRequestsModel.SWF_RUN_ID_KEY;
    public static final String DEVICE_SCOPE_KEY = MitigationRequestsModel.DEVICE_SCOPE_KEY;
    public static final String WORKFLOW_STATUS_KEY = MitigationRequestsModel.WORKFLOW_STATUS_KEY;
    public static final String MITIGATION_NAME_KEY = MitigationRequestsModel.MITIGATION_NAME_KEY;
    public static final String MITIGATION_VERSION_KEY = MitigationRequestsModel.MITIGATION_VERSION_KEY;
    public static final String MITIGATION_TEMPLATE_KEY = MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY;
    public static final String SERVICE_NAME_KEY = MitigationRequestsModel.SERVICE_NAME_KEY;
    public static final String REQUEST_DATE_KEY = MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY;
    public static final String REQUEST_TYPE_KEY = MitigationRequestsModel.REQUEST_TYPE_KEY;
    public static final String USERNAME_KEY = MitigationRequestsModel.USER_NAME_KEY;
    public static final String TOOL_NAME_KEY = MitigationRequestsModel.TOOL_NAME_KEY;
    public static final String USER_DESC_KEY = MitigationRequestsModel.USER_DESCRIPTION_KEY;
    public static final String RELATED_TICKETS_KEY = MitigationRequestsModel.RELATED_TICKETS_KEY;
    public static final String LOCATIONS_KEY = MitigationRequestsModel.LOCATIONS_KEY;
    public static final String MITIGATION_DEFINITION_KEY = MitigationRequestsModel.MITIGATION_DEFINITION_KEY;
    public static final String MITIGATION_DEFINITION_HASH_KEY = MitigationRequestsModel.MITIGATION_DEFINITION_HASH_KEY;
    public static final String NUM_PRE_DEPLOY_CHECKS_KEY = MitigationRequestsModel.NUM_PRE_DEPLOY_CHECKS_KEY;
    public static final String PRE_DEPLOY_CHECKS_DEFINITION_KEY = "PreDeployChecks";
    public static final String NUM_POST_DEPLOY_CHECKS_KEY = MitigationRequestsModel.NUM_POST_DEPLOY_CHECKS_KEY;
    public static final String POST_DEPLOY_CHECKS_DEFINITION_KEY = "PostDeployChecks";
    public static final String UPDATE_WORKFLOW_ID_KEY = MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY;
    
    // Below is a list of relevant keys from the Active Mitigations table.
    public static final String DELETION_DATE_KEY = "DeletionDate";
    public static final String DEFUNCT_DATE_KEY = "DefunctDate";
    
    // Keys for TSDMetrics.
    private static final String NUM_DDB_PUT_ITEM_ATTEMPTS_KEY = "NumPutAttempts";
    private static final String NUM_DDB_QUERY_ATTEMPTS_KEY = "NumDDBQueryAttempts";
    private static final String NUM_DDB_UPDATE_ITEM_ATTEMPTS_KEY = "NumDDBUpdateAttempts";
    private static final String NUM_DDB_GET_ITEM_ATTEMPTS_KEY = "NumDDBGetAttempts";
    
    // Retry and sleep configs.
    private static final int DDB_QUERY_MAX_ATTEMPTS = 3;
    private static final int DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    protected static final int DDB_PUT_ITEM_MAX_ATTEMPTS = 3;
    private static final int DDB_PUT_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Slightly higher number of retries for updating item, since we've already done a whole lot of work for creating the item.
    protected static final int DDB_UPDATE_ITEM_MAX_ATTEMPTS = 5;
    private static final int DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    public static final int UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS = 0;
    
    protected static final String UNEDITED_MITIGATIONS_LSI_NAME = MitigationRequestsModel.UNEDITED_MITIGATIONS_LSI_NAME;
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();
    
    private final AmazonDynamoDBClient dynamoDBClient;
    protected final String mitigationRequestsTableName;
    protected final String activeMitigationsTableName;
    
    public DDBBasedRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain) {
        Validate.notNull(dynamoDBClient);
        this.dynamoDBClient = dynamoDBClient;
        
        Validate.notEmpty(domain);
        this.mitigationRequestsTableName = MITIGATION_REQUEST_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.activeMitigationsTableName = ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX + domain.toUpperCase();
    }

    /**
     * Helper method to store a request into DDB.
     * @param request Request to be stored.
     * @param locations Set of String representing the locations where this request applies.
     * @param deviceNameAndScope DeviceNameAndScope for this request.
     * @param workflowId WorkflowId to use for storing this request.
     * @param requestType Indicates the type of request made to the service.
     * @param mitigationVersion Version number to be associated with this mitigation to be stored.
     * @param metrics
     */
    protected void storeRequestInDDB(MitigationModificationRequest request, Set<String> locations, DeviceNameAndScope deviceNameAndScope, long workflowId, 
                                     RequestType requestType, int mitigationVersion, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.storeRequestInDDB");
        int numAttempts = 0;
        try {
            Map<String, AttributeValue> attributeValuesInItem = generateAttributesToStore(request, locations, deviceNameAndScope, workflowId, requestType, mitigationVersion);
            Map<String, ExpectedAttributeValue> expectedConditions = generateExpectedConditions(deviceNameAndScope, workflowId);
            
            // Try for a fixed number of times to store the item into DDB. If we succeed, we simply return, else we exit the loop and throw back an exception.
            while (numAttempts++ < DDB_PUT_ITEM_MAX_ATTEMPTS) {
                try {
                    putItemInDDB(attributeValuesInItem, expectedConditions, subMetrics);
                    return;
                } catch (Exception ex) {
                    String msg = "Caught exception when inserting item: " + attributeValuesInItem + " into table: " + mitigationRequestsTableName + 
                                 ". Num attempts so far: " + numAttempts + " " + ReflectionToStringBuilder.toString(request) + " for locations: " + locations;
                    LOG.warn(msg, ex);
                    
                    if (numAttempts < DDB_PUT_ITEM_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(getSleepMillisMultiplierOnPutRetry() * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;
            
            String msg = "Unable to insert item: " + attributeValuesInItem + " into table: " + mitigationRequestsTableName + " after " + numAttempts + 
                         "attempts for request: " + ReflectionToStringBuilder.toString(request) + " for locations: " + locations;
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_DDB_PUT_ITEM_ATTEMPTS_KEY, numAttempts);
            subMetrics.end();
        }
    }
    
    /**
     * Stores the attributeValues into the DDBTable. Delegates the call to DynamoDBHelper for calling the putItem DDB API.
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
     * Generates the Map of String - representing the attributeName, to AttributeValue - representing the value to store for this attribute.
     * Protected for unit-testing. 
     * @param request Request to be persisted.
     * @param locations Set of String where this request applies.
     * @param deviceNameAndScope DeviceNameAndScope corresponding to this request.
     * @param workflowId WorkflowId to store this request with.
     * @param requestType Type of request (eg: create/edit/delete).
     * @param mitigationVersion Version number to use for storing the mitigation in this request.
     * @return Map of String (attributeName) -> AttributeValue.
     */
    protected Map<String, AttributeValue> generateAttributesToStore(MitigationModificationRequest request, Set<String> locations, DeviceNameAndScope deviceNameAndScope, 
                                                                    long workflowId, RequestType requestType, int mitigationVersion) {
        Map<String, AttributeValue> attributesInItemToStore = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(deviceNameAndScope.getDeviceName().name());
        attributesInItemToStore.put(DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(workflowId));
        attributesInItemToStore.put(WORKFLOW_ID_KEY, attributeValue);
        
        attributeValue = new AttributeValue(deviceNameAndScope.getDeviceScope().name());
        attributesInItemToStore.put(DEVICE_SCOPE_KEY, attributeValue);
        
        attributeValue = new AttributeValue(WorkflowStatus.RUNNING);
        attributesInItemToStore.put(WORKFLOW_STATUS_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getMitigationName());
        attributesInItemToStore.put(MITIGATION_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(mitigationVersion));
        attributesInItemToStore.put(MITIGATION_VERSION_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getMitigationTemplate());
        attributesInItemToStore.put(MITIGATION_TEMPLATE_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getServiceName());
        attributesInItemToStore.put(SERVICE_NAME_KEY, attributeValue);
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        attributeValue = new AttributeValue().withN(String.valueOf(now.getMillis()));
        attributesInItemToStore.put(REQUEST_DATE_KEY, attributeValue);
        
        attributeValue = new AttributeValue(requestType.name());
        attributesInItemToStore.put(REQUEST_TYPE_KEY, attributeValue);
        
        MitigationActionMetadata metadata = request.getMitigationActionMetadata();
        attributeValue = new AttributeValue(metadata.getUser());
        attributesInItemToStore.put(USERNAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getToolName());
        attributesInItemToStore.put(TOOL_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getDescription());
        attributesInItemToStore.put(USER_DESC_KEY, attributeValue);
        
        // Related tickets isn't a required attribute, hence checking if it has been provided before creating a corresponding AttributeValue for it.
        if ((metadata.getRelatedTickets() != null) && !metadata.getRelatedTickets().isEmpty()) {
            attributeValue = new AttributeValue().withSS(metadata.getRelatedTickets());
            attributesInItemToStore.put(RELATED_TICKETS_KEY, attributeValue);
        }
        
        attributeValue = new AttributeValue().withSS(locations);
        attributesInItemToStore.put(LOCATIONS_KEY, attributeValue);
        
        // Specifying MitigationDefinition only makes sense for the Create/Edit requests and hence extracting it only for such requests.
        if ((requestType == RequestType.CreateRequest) || (requestType == RequestType.EditRequest)) {
            MitigationDefinition mitigationDefinition = null;
            
            if (requestType == RequestType.CreateRequest) {
                mitigationDefinition = ((CreateMitigationRequest) request).getMitigationDefinition();
            } else {
                mitigationDefinition = ((EditMitigationRequest) request).getMitigationDefinition();
            }
            
            String mitigationDefinitionJSONString = getJSONDataConverter().toData(mitigationDefinition); 
            attributeValue = new AttributeValue(mitigationDefinitionJSONString);
            attributesInItemToStore.put(MITIGATION_DEFINITION_KEY, attributeValue);
            
            int mitigationDefinitionHashCode = mitigationDefinitionJSONString.hashCode();
            attributeValue = new AttributeValue().withN(String.valueOf(mitigationDefinitionHashCode));
            attributesInItemToStore.put(MITIGATION_DEFINITION_HASH_KEY, attributeValue);
        }
        
        List<MitigationDeploymentCheck> deploymentChecks = request.getPreDeploymentChecks();
        if ((deploymentChecks == null) || deploymentChecks.isEmpty()) {
            attributeValue = new AttributeValue().withN(String.valueOf(0));
            attributesInItemToStore.put(NUM_PRE_DEPLOY_CHECKS_KEY, attributeValue);
        } else {
            attributeValue = new AttributeValue().withN(String.valueOf(deploymentChecks.size()));
            attributesInItemToStore.put(NUM_PRE_DEPLOY_CHECKS_KEY, attributeValue);
            
            String preDeployChecksAsJSONString = getJSONDataConverter().toData(deploymentChecks);
            attributeValue = new AttributeValue(preDeployChecksAsJSONString);
            attributesInItemToStore.put(PRE_DEPLOY_CHECKS_DEFINITION_KEY, attributeValue);
        }
        
        deploymentChecks = request.getPostDeploymentChecks();
        if ((deploymentChecks == null) || deploymentChecks.isEmpty()) {
            attributeValue = new AttributeValue().withN(String.valueOf(0));
            attributesInItemToStore.put(NUM_POST_DEPLOY_CHECKS_KEY, attributeValue);
        } else {
            attributeValue = new AttributeValue().withN(String.valueOf(deploymentChecks.size()));
            attributesInItemToStore.put(NUM_POST_DEPLOY_CHECKS_KEY, attributeValue);
            
            String postDeployChecksAsJSONString = getJSONDataConverter().toData(deploymentChecks);
            attributeValue = new AttributeValue(postDeployChecksAsJSONString);
            attributesInItemToStore.put(POST_DEPLOY_CHECKS_DEFINITION_KEY, attributeValue);
        }
        
        attributeValue = new AttributeValue().withN(String.valueOf(UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS));
        attributesInItemToStore.put(UPDATE_WORKFLOW_ID_KEY, attributeValue);
        
        return attributesInItemToStore;
    }
    
    /**
     * Helper method to create the expected conditions when inserting record into the MitigationRequests table.
     * The conditions to check for is to ensure we don't already have a record with the same hashKey (deviceName) and rangeKey (workflowId).
     * @param deviceNameAndScope DeviceNameAndScope enum, used to obtain the deviceName for the expectation condition.
     * @param workflowId WorkflowId to be used in the expectation condition.
     * @return Map of String (attribute name) to ExpectedAttributeValue to represent the requirement of non-existence of the keys of this map.
     */
    protected Map<String, ExpectedAttributeValue> generateExpectedConditions(@NonNull DeviceNameAndScope deviceNameAndScope, long workflowId) {
        Map<String, ExpectedAttributeValue> expectedCondition = new HashMap<>();
        
        ExpectedAttributeValue notExistsCondition = new ExpectedAttributeValue(false);
        expectedCondition.put(DEVICE_NAME_KEY, notExistsCondition);
        expectedCondition.put(WORKFLOW_ID_KEY, notExistsCondition);
        
        return expectedCondition;
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
            Map<String, Condition> keyConditions = getKeysForDeviceAndWorkflowId(deviceName, maxWorkflowIdOnLastAttempt);
            
            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryRequest request = new QueryRequest();
                request.setAttributesToGet(Collections.singleton(WORKFLOW_ID_KEY));
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
                request.addQueryFilterEntry(DEVICE_SCOPE_KEY, condition);
                
                QueryResult queryResult = null;
                int numAttempts = 0;
                // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop endsthrow back an exception.
                while (numAttempts++ < DDB_QUERY_MAX_ATTEMPTS) {
                    try {
                        subMetrics.addOne(NUM_DDB_QUERY_ATTEMPTS_KEY);
                        queryResult = queryDynamoDB(request, subMetrics);
                        break;
                    } catch (Exception ex) {
                        String msg = "Caught Exception when trying to query for active mitigations for device: " + deviceName + " for deviceScope: " + deviceScope + 
                                     " maxWorkflowIdOnLastAttempt: " + maxWorkflowIdOnLastAttempt + " with consistentRead and keyConditions: " + keyConditions + ". Attempt so far: " + numAttempts;
                        LOG.warn(msg, ex);
                    }
                    
                    if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
                
                if (queryResult == null) {
                    // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
                    numAttempts = numAttempts - 1;
    
                    String msg = "Unable to query DDB for active mitigations for device: " + deviceName + " for deviceScope: " + deviceScope + " maxWorkflowIdOnLastAttempt: " + 
                                 maxWorkflowIdOnLastAttempt + " with consistentRead and keyConditions: " + keyConditions + ". Total NumAttempts: " + numAttempts;
                    LOG.warn(msg);
                    throw new RuntimeException(msg);
                }
                
                lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                if (queryResult.getCount() > 0) {
                    // Since we query the primary key and set the index to be iterated in the reverse order, the first result we get back is the max workflowId.
                    return Long.parseLong(queryResult.getItems().get(0).get(WORKFLOW_ID_KEY).getN());
                }
            } while (lastEvaluatedKey != null);
            
            return null;
        } finally {
            subMetrics.end();
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
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.getActiveMitigationsForDevice");
        int numAttempts = 0;
        try {
            // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop endsthrow back an exception.
            while (numAttempts++ < DDB_QUERY_MAX_ATTEMPTS) {
                try {
                    QueryRequest request = new QueryRequest();
                    request.setAttributesToGet(attributesToGet);
                    request.setTableName(mitigationRequestsTableName);
                    request.setConsistentRead(true);
                    request.setKeyConditions(keyConditions);
                    
                    if ((queryFilters != null) && !queryFilters.isEmpty()) {
                        request.withQueryFilter(queryFilters);
                    }
                    
                    if (lastEvaluatedKey != null) {
                        request.setExclusiveStartKey(lastEvaluatedKey);
                    }
                    if ((indexToUse != null) && !indexToUse.isEmpty()) {
                        request.setIndexName(indexToUse);
                    }
                    
                    // Filter out any records whose DeviceScope isn't the same.
                    if ((request.getQueryFilter() == null) || !request.getQueryFilter().containsKey(DEVICE_SCOPE_KEY)) {
                        AttributeValue deviceScopeAttrVal = new AttributeValue(deviceScope);
                        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(Arrays.asList(deviceScopeAttrVal));
                        request.addQueryFilterEntry(DEVICE_SCOPE_KEY, condition);
                    }
                    
                    return queryDynamoDB(request, subMetrics);
                    
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to query for active mitigations for device: " + deviceName + 
                                 " attributesToGet " + attributesToGet + " with consistentRead and keyConditions: " + keyConditions +
                                 ". Attempt so far: " + numAttempts;
                    LOG.warn(msg, ex);
                }

                if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                    try {
                        Thread.sleep(DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                    } catch (InterruptedException ignored) {}
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = "Unable to query DDB for active mitigations for device: " + deviceName + " attributesToGet " + attributesToGet + 
                         " with consistentRead: " + "and keyConditions: " + keyConditions + ". Total NumAttempts: " + numAttempts;
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_DDB_QUERY_ATTEMPTS_KEY, numAttempts);
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for recording the SWFRunId corresponding to the workflow request just created in DDB.
     * @param deviceName DeviceName corresponding to the workflow being run.
     * @param workflowId WorkflowId for the workflow being run.
     * @param runId SWF assigned runId for the running instance of this workflow.
     * @param metrics
     */
    public void updateRunIdForWorkflowRequest(@Nonnull String deviceName, long workflowId, @Nonnull String runId, @Nonnull TSDMetrics metrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(workflowId > 0);
        Validate.notEmpty(runId);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.updateRunIdForWorkflowRequest");
        int numAttempts = 0;
        try {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(DEVICE_NAME_KEY, new AttributeValue(deviceName));
            key.put(WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(workflowId)));
            
            Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
            attributeUpdates.put(SWF_RUN_ID_KEY, new AttributeValueUpdate(new AttributeValue(runId), AttributeAction.PUT));

            Map<String, ExpectedAttributeValue> expected = new HashMap<>();
            ExpectedAttributeValue absentValueExpectation = new ExpectedAttributeValue();
            absentValueExpectation.setExists(false);
            expected.put(SWF_RUN_ID_KEY, absentValueExpectation);
            
            // Attempt to update DDB for a fixed number of times.
            while (numAttempts++ < DDB_UPDATE_ITEM_MAX_ATTEMPTS) {
                try {
                    updateItemInDynamoDB(attributeUpdates, key, expected);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    String msg = "For workflowId: " + workflowId + " for device: " + deviceName + " attempted runId update: " + runId + 
                                 ", but caught a ConditionalCheckFailedException indicating runId was already updated!";
                    LOG.error(msg, ex);
                    throw new RuntimeException(msg, ex);
                } catch (Exception ex) {
                    String msg = "Caught Exception when updating runId to :" + runId + " for device: " + deviceName + 
                                 " for workflowId: " + workflowId + ". Attempts so far: " + numAttempts;
                    LOG.warn(msg, ex);
    
                   if (numAttempts < DDB_UPDATE_ITEM_MAX_ATTEMPTS) {
                       try {
                           Thread.sleep(getSleepMillisMultiplierOnUpdateRetry() * numAttempts);
                       } catch (InterruptedException ignored) {}
                   }
                }
            }
            
            String msg = "Unable to update runId to :" + runId + " for device: " + deviceName + 
                         " for workflowId: " + workflowId + " after " + numAttempts + " number of attempts.";
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_DDB_UPDATE_ITEM_ATTEMPTS_KEY, numAttempts);
            subMetrics.end();
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
            while (numAttempts++ < DDB_QUERY_MAX_ATTEMPTS) {
                try {
                    return getItemInDynamoDB(keys, subMetrics);
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to query DynamoDB. Attempts so far: " + numAttempts;
                    LOG.warn(msg, ex);
                    if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = "Unable to get the item from DDB. Total NumAttempts: " + numAttempts;
            LOG.warn(msg);
            throw new RuntimeException(msg);
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
    protected QueryResult queryRequestsInDDB(QueryRequest request, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.queryRequestsInDDB");
        int numAttempts = 0;
        try {
            // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop endsthrow back an exception.
            while (numAttempts++ < DDB_QUERY_MAX_ATTEMPTS) {
                try {
                    return queryDynamoDB(request, subMetrics);
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to query DynamoDB. Attempts so far: " + numAttempts;
                    LOG.warn(msg, ex);
                    if (numAttempts < DDB_QUERY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = "Unable to query DDB. Total NumAttempts: " + numAttempts;
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_DDB_QUERY_ATTEMPTS_KEY, numAttempts);
            subMetrics.end();
        }
    }
    
    
    /**
     * Issues a query against the DDBTable. Delegates the call to DynamoDBHelper for calling the query DDB API.
     * Protected for unit tests.
     * @param request Request to be queried.
     * @param metrics
     * @return QueryResult corresponding to the result of the query.
     */
    protected QueryResult queryDynamoDB(QueryRequest request, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHelper.queryDynamoDB");
        boolean handleRetries = false;
        try {
            return DynamoDBHelper.queryItemAttributesFromTable(dynamoDBClient, request, handleRetries);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Issues a get operation against the DDBTable. Delegates the call to DynamoDBHelper for calling the query DDB API.
     * Protected for unit tests.
     * @param keys Primary key attribute values to search for.
     * @param metrics
     * @return GetItemResult corresponding to the result of the get request.
     */
    protected GetItemResult getItemInDynamoDB(Map<String, AttributeValue> keys, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.queryDynamoDB");
        boolean handleRetries = false;
        try {
            return DynamoDBHelper.getItemAttributesFromTable(dynamoDBClient, mitigationRequestsTableName, keys, handleRetries);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Issues a get operation against the DDBTable. Delegates the call to DynamoDBHelper for calling the query DDB API.
     * Protected for unit tests.
     * @param keys Primary key attribute values to search for.
     * @param metrics
     * @return GetItemResult corresponding to the result of the get request.
     */
    protected GetItemResult getItemInDynamoDB(Map<String, AttributeValue> keys/*, TSDMetrics metrics*/) {
        //TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.queryDynamoDB");
        try {
            return DynamoDBHelper.getItemAttributesFromTable(dynamoDBClient, mitigationRequestsTableName, keys, false);
        } finally {
            //subMetrics.end();
        }
    }
    
    /**
     * Helper to return a JSONDataConverter. We use the data converter provided by the SWF dependency.
     * Protected for unit-testing.
     * @return An instance of JSONDataConverter.
     */
    protected DataConverter getJSONDataConverter() {
        return jsonDataConverter;
    }
    
    /**
     * Helper to return the multiplier for sleeping on each failure when calling PutItem on DDB.
     * Protected for unit-testing, to allow injecting this value. 
     * @return long multiplier for sleeping on each failure when calling PutItem on DDB.
     */
    protected int getSleepMillisMultiplierOnPutRetry() {
        return DDB_PUT_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER;
    }
    
    /**
     * Helper to return the multiplier for sleeping on each failure when calling UpdateItem on DDB.
     * Protected for unit-testing, to allow injecting this value. 
     * @return long multiplier for sleeping on each failure when calling UpdateItem on DDB.
     */
    protected int getSleepMillisMultiplierOnUpdateRetry() {
        return DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER;
    }
    
    /**
     * Delegates the call to DynamoDBHelper. protected to allow for unit-testing.
     * @param tableName TableName where the update needs to happen.
     * @param attributeUpdates AttributeValues that represent the update that needs to happen.
     * @param key Represents the item key that needs to be updates.
     * @param expected Represents the conditions that must exist before the update is run.
     */
    protected void updateItemInDynamoDB(Map<String, AttributeValueUpdate> attributeUpdates, Map<String, AttributeValue> key, Map<String, ExpectedAttributeValue> expected) {
        DynamoDBHelper.updateItem(dynamoDBClient, mitigationRequestsTableName, attributeUpdates, key, expected, null, null, null);
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
    
    /**
     * Generate the keys for querying active mitigations for the device passed as input. Protected for unit-testing.
     * An active mitigation is one whose UpdateWorkflowId column hasn't been updated in the MitigationRequests table.
     * @param deviceName Device for whom we need to find the active mitigations.
     * @return Keys for querying active mitigations for the device passed as input.
     */
    protected Map<String, Condition> getKeysForActiveMitigationsForDevice(String deviceName) {
        Map<String, Condition> keyConditions = new HashMap<>();
        
        Set<AttributeValue> keyValues = Collections.singleton(new AttributeValue(deviceName));
        
        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, condition);

        keyValues = Collections.singleton(new AttributeValue().withN(String.valueOf(UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS)));
        
        condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY, condition);

        return keyConditions;
    }

    /**
     * Generate the keys for querying mitigations for the device passed as input and whose workflowIds are equal or greater than the workflowId passed as input.
     * Protected for unit-testing
     * @param deviceName Device for whom we need to find the active mitigations.
     * @param workflowId WorkflowId which we need to constraint our query by. We should be querying for existing mitigations whose workflowIds are >= this value.
     * @return Keys for querying mitigations for the device passed as input with workflowIds >= the workflowId passed as input above.
     */
    protected Map<String, Condition> getKeysForDeviceAndWorkflowId(String deviceName, Long workflowId) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Set<AttributeValue> keyValues = Collections.singleton(new AttributeValue(deviceName));

        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, condition);

        if (workflowId != null) {
            keyValues = Collections.singleton(new AttributeValue().withN(String.valueOf(workflowId)));
            condition = new Condition();
            condition.setComparisonOperator(ComparisonOperator.GE);
            condition.setAttributeValueList(keyValues);
            keyConditions.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, condition);
        }

        return keyConditions;
    }
    
    /**
     * Helper to create the QueryFilter to use when querying for MaxWorkflowId.
     * @param deviceScope Device scope of the device for which we need to determine the max workflowId.
     * @return Map <String, Condition> representing the queryFilter to use when querying DDB.
     */
    protected Map<String, Condition> createQueryFiltersForMaxWorkflowId(String deviceScope) {
        Map<String, Condition> queryFilters = new HashMap<>();
        
        AttributeValue attrVal = new AttributeValue(deviceScope);
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(DEVICE_SCOPE_KEY, condition);
        
        return queryFilters;
    }

}
