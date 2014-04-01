package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazon.aws158.commons.dynamo.DynamoDBHelper;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;

/**
 * DDBBasedRequestStorageHandler is an abstract class meant to be a helper for concrete request storage handler implementations.
 * The requests are stores in the MITIGATION_REQUESTS table, where we have a separate table per domain (eg: MITIGATION_REQUESTS_{BETA/GAMMA/PROD}).
 * Details of this table can be found here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#MITIGATION_REQUESTS
 * 
 */
public abstract class DDBBasedRequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedRequestStorageHandler.class);
    
    public static final String MITIGATION_REQUEST_TABLE_NAME_PREFIX = "MITIGATION_REQUESTS_";
    
    // Below is a list of all the attributes we store in the MitigationRequests table.
    public static final String DEVICE_NAME_KEY = "DeviceName";
    public static final String WORKFLOW_ID_KEY = "WorkflowId";
    public static final String DEVICE_SCOPE_KEY = "DeviceScope";
    public static final String WORKFLOW_STATUS_KEY = "WorkflowStatus";
    public static final String MITIGATION_NAME_KEY = "MitigationName";
    public static final String MITIGATION_VERSION_KEY = "MitigationVersion";
    public static final String MITIGATION_TEMPLATE_KEY = "MitigationTemplate";
    public static final String SERVICE_NAME_KEY = "ServiceName";
    public static final String REQUEST_DATE_KEY = "RequestDate";
    public static final String REQUEST_TYPE_KEY = "RequestType";
    public static final String USERNAME_KEY = "UserName";
    public static final String TOOL_NAME_KEY = "ToolName";
    public static final String USER_DESC_KEY = "UserDescription";
    public static final String RELATED_TICKETS_KEY = "RelatedTickets";
    public static final String LOCATIONS_KEY = "Locations";
    public static final String MITIGATION_DEFINITION_KEY = "MitigationDefinition";
    public static final String MITIGATION_DEFINITION_HASH_KEY = "MitigationDefinitionHash";
    public static final String NUM_PRE_DEPLOY_CHECKS_KEY = "NumPreDeployChecks";
    public static final String PRE_DEPLOY_CHECKS_DEFINITION_KEY = "PreDeployChecks";
    public static final String NUM_POST_DEPLOY_CHECKS_KEY = "NumPostDeployChecks";
    public static final String POST_DEPLOY_CHECKS_DEFINITION_KEY = "PostDeployChecks";
    public static final String UPDATE_WORKFLOW_ID_KEY = "UpdateWorkflowId";
    
    // Keys for TSDMetrics.
    private static final String NUM_DDB_PUT_ITEM_ATTEMPTS_KEY = "NumPutAttempts";
    private static final String NUM_DDB_QUERY_ATTEMPTS_KEY = "NumDDBQueryAttempts";
    
    // Retry and sleep configs.
    private static final int DDB_QUERY_MAX_ATTEMPTS = 3;
    private static final int DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    protected static final int DDB_PUT_ITEM_MAX_ATTEMPTS = 3;
    private static final int DDB_PUT_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    public final static int UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS = 0;
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();
    
    private final AmazonDynamoDBClient dynamoDBClient;
    private final String mitigationRequestsTableName;
    
    public DDBBasedRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain) {
        Validate.notNull(dynamoDBClient);
        this.dynamoDBClient = dynamoDBClient;
        
        Validate.notEmpty(domain);
        this.mitigationRequestsTableName = MITIGATION_REQUEST_TABLE_NAME_PREFIX + domain.toUpperCase();
    }

    /**
     * Helper method to store a request into DDB.
     * @param request Request to be stored.
     * @param deviceNameAndScope DeviceNameAndScope for this request.
     * @param workflowId WorkflowId to use for storing this request.
     * @param requestType Indicates the type of request made to the service.
     * @param mitigationVersion Version number to be associated with this mitigation to be stored.
     * @param metrics
     */
    protected void storeRequestInDDB(MitigationModificationRequest request, DeviceNameAndScope deviceNameAndScope, long workflowId, 
                                     String requestType, int mitigationVersion, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.storeRequestInDDB");
        int numAttempts = 0;
        try {
            Map<String, AttributeValue> attributeValuesInItem = generateAttributesToStore(request, deviceNameAndScope, workflowId, requestType, mitigationVersion);
            
            // Try for a fixed number of times to store the item into DDB. If we succeed, we simply return, else we exit the loop and throw back an exception.
            while (numAttempts++ < DDB_PUT_ITEM_MAX_ATTEMPTS) {
                try {
                    putItemInDDB(attributeValuesInItem, subMetrics);
                    return;
                } catch (Exception ex) {
                    String msg = "Caught exception when inserting item: " + attributeValuesInItem + " into table: " + mitigationRequestsTableName + 
                                 ". Num attempts so far: " + numAttempts + " " + ReflectionToStringBuilder.toString(request);
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
                         "attempts for request: " + ReflectionToStringBuilder.toString(request);
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
    protected void putItemInDDB(Map<String, AttributeValue> attributeValues, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageHandler.putItemInDDB");
        try {
            DynamoDBHelper.putItemAttributesToTable(dynamoDBClient, mitigationRequestsTableName, attributeValues, false);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Generates the Map of String - representing the attributeName, to AttributeValue - representing the value to store for this attribute.
     * Protected for unit-testing. 
     * @param request Request to be persisted.
     * @param deviceNameAndScope DeviceNameAndScope corresponding to this request.
     * @param workflowId WorkflowId to store this request with.
     * @param requestType Type of request (eg: create/edit/delete).
     * @param mitigationVersion Version number to use for storing the mitigation in this request.
     * @return Map of String (attributeName) -> AttributeValue.
     */
    protected Map<String, AttributeValue> generateAttributesToStore(MitigationModificationRequest request, DeviceNameAndScope deviceNameAndScope, 
                                                                    long workflowId, String requestType, int mitigationVersion) {
        Map<String, AttributeValue> attributesInItemToStore = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(deviceNameAndScope.getDeviceName().name());
        attributesInItemToStore.put(DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(workflowId));
        attributesInItemToStore.put(WORKFLOW_ID_KEY, attributeValue);
        
        attributeValue = new AttributeValue(deviceNameAndScope.getDeviceScope().name());
        attributesInItemToStore.put(DEVICE_SCOPE_KEY, attributeValue);
        
        attributeValue = new AttributeValue(WorkflowStatus.CREATED);
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
        attributeValue = new AttributeValue(String.valueOf(now.getMillis()));
        attributesInItemToStore.put(REQUEST_DATE_KEY, attributeValue);
        
        attributeValue = new AttributeValue(requestType);
        attributesInItemToStore.put(REQUEST_TYPE_KEY, attributeValue);
        
        MitigationActionMetadata metadata = request.getMitigationActionMetadata();
        attributeValue = new AttributeValue(metadata.getUser());
        attributesInItemToStore.put(USERNAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getToolName());
        attributesInItemToStore.put(TOOL_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getDescription());
        attributesInItemToStore.put(USER_DESC_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withSS(metadata.getRelatedTickets());
        attributesInItemToStore.put(RELATED_TICKETS_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withSS(request.getLocation());
        attributesInItemToStore.put(LOCATIONS_KEY, attributeValue);
        
        String mitigationDefinitionJSONString = getJSONDataConverter().toData(request.getMitigationDefinition()); 
        attributeValue = new AttributeValue(mitigationDefinitionJSONString);
        attributesInItemToStore.put(MITIGATION_DEFINITION_KEY, attributeValue);
        
        int mitigationDefinitionHashCode = mitigationDefinitionJSONString.hashCode();
        attributeValue = new AttributeValue().withN(String.valueOf(mitigationDefinitionHashCode));
        attributesInItemToStore.put(MITIGATION_DEFINITION_HASH_KEY, attributeValue);
        
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
     * Called by the concrete implementations of this StorageHandler to find all the currently active mitigations for a device.
     * @param deviceName Device corresponding to whom all active mitigations need to be determined.
     * @param attributesToGet Set of attributes to retrieve for each active mitigation.
     * @param keyConditions Map of String (attributeName) and Condition - Condition represents constraint on the attribute. Eg: >= 5.
     * @param lastEvaluatedKey Last evaluated key, to handle paginated response.
     * @param metrics
     * @return QueryResult representing the result from issuing this query to DDB.
     */
    protected QueryResult getActiveMitigationsForDevice(String deviceName, Set<String> attributesToGet, Map<String, Condition> keyConditions, 
                                                        Map<String, AttributeValue> lastEvaluatedKey, TSDMetrics metrics) {
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
                    if (lastEvaluatedKey != null) {
                        request.setExclusiveStartKey(lastEvaluatedKey);
                    }
                    return queryDynamoDB(request, subMetrics);
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to query for active mitigations for device: " + deviceName + 
                                 " attributesToGet " + attributesToGet + " with consistentRead and keyConditions: " + keyConditions +
                                 ". Attempt so far: " + numAttempts;
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
     * Issues a query against the DDBTable. Delegates the call to DynamoDBHelper for calling the query DDB API.
     * Protected for unit tests.
     * @param request Request to be queried.
     * @param metrics
     * @return QueryResult corresponding to the result of the query.
     */
    protected QueryResult queryDynamoDB(QueryRequest request, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.queryDynamoDB");
        try {
            return DynamoDBHelper.queryItemAttributesFromTable(dynamoDBClient, request, false);
        } finally {
            subMetrics.end();
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

}
