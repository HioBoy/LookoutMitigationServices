package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.dynamo.DynamoDBHelper;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

/**
 * DDBBasedMitigationStorageHandler is a helper class for pulling information from the MitigationInstances table.
 * 
 */
public abstract class DDBBasedMitigationStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedRequestStorageHandler.class);
    
    public static final String MITIGATION_INSTANCES_TABLE_NAME_PREFIX = "MITIGATION_INSTANCES_";
    public static final String ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX = "ACTIVE_MITIGATIONS_";
    
    // Below is a list of all the attributes we store in the MitigationInstances table.
    public static final String DEVICE_WORKFLOW_ID_KEY = "DeviceWorkflowId";
    public static final String LOCATION_KEY = "Location";
    public static final String CREATE_DATE_KEY = "CreateDate";
    public static final String DEVICE_NAME_KEY = "DeviceName";
    public static final String MITIGATION_NAME_KEY = "MitigationName";
    public static final String MITIGATION_STATUS_KEY = "MitigationStatus";
    public static final String MITIGATION_TEMPLATE_KEY = "MitigationTemplate";
    public static final String MITIGATION_VERSION_KEY = "MitigationVersion";
    public static final String NUM_POST_DEPLOY_CHECKS_KEY = "NumPostDeployChecks";
    public static final String NUM_PRE_DEPLOY_CHECKS_KEY = "NumPreDeployChecks";
    public static final String SCHEDULING_STATUS_KEY = "SchedulingStatus";
    public static final String SERVICE_NAME_KEY = "ServiceName";
    public static final String STATUS_PRE_DEPLOY_CHECK_KEY_1 = "StatusPreDeployCheck-1";
    public static final String BLOCKING_DEVICE_WORKFLOW_ID = "BlockingDeviceWorkflowId";
    public static final String STATUS_POST_DEPLOY_CHECK_KEY = "StatusPostDeployCheck-1";
    public static final String STATUS_PRE_DEPLOY_CHECK_KEY_2 = "StatusPreDeployCheck-2";
    public static final String DEPLOYMENT_HISTORY_KEY = "DeploymentHistory";
    
    // Retry and sleep configs.
    private static final int DDB_QUERY_MAX_ATTEMPTS = 3;
    private static final int DDB_QUERY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Keys for TSDMetrics
    private static final String NUM_DDB_QUERY_ATTEMPTS_KEY = "NumDDBQueryAttempts";
    
    private final AmazonDynamoDBClient dynamoDBClient;
    protected final String mitigationInstancesTableName;
    protected final String activeMitigationsTableName;
    
    public DDBBasedMitigationStorageHandler(@NonNull AmazonDynamoDBClient dynamoDBClient, @NonNull String domain) {
        this.dynamoDBClient = dynamoDBClient;
        this.mitigationInstancesTableName = MITIGATION_INSTANCES_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.activeMitigationsTableName = ACTIVE_MITIGATIONS_TABLE_NAME_PREFIX + domain.toUpperCase();
    }
    
    /**
     * Called by the concrete implementations of this StorageHandler to query DDB.
     * @param request QueryRequest object containing the information to use for querying.
     * @param metrics
     * @return QueryResult representing the result from issuing this query to DDB.
     */
    protected QueryResult queryMitigationsInDDB(QueryRequest request, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedMitigationStorageHandler.queryMitigationsInDDB");
        int numAttempts = 0;
        try {
            // Attempt to query DDB for a fixed number of times. If query was successful, return the QueryResult, else when the loop ends throw back an exception.
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
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedMitigationStorageHandler.queryDynamoDB");
        boolean handleRetries = false;
        try {
            return DynamoDBHelper.queryItemAttributesFromTable(dynamoDBClient, request, handleRetries);
        } finally {
            subMetrics.end();
        }
    }
}
