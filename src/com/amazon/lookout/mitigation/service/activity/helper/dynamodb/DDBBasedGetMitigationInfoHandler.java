package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Sets;

public class DDBBasedGetMitigationInfoHandler extends DDBBasedMitigationStorageHandler implements MitigationInstanceInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedGetMitigationInfoHandler.class);
    
    // Keys for TSDMetric properties.
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_INSTANCE_STATUSES = "NumGetMitigationInstanceStatusesAttempts";
    
    public DDBBasedGetMitigationInfoHandler(AmazonDynamoDBClient dynamoDBClient, String domain) {
        super(dynamoDBClient, domain);
    }
    
    /**
     * Generate a list of MitigationInstanceStatus.
     * @param deviceName The name of the device.
     * @param jobId The job id of the mitigation instance.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<MitigationInstanceStatus> getMitigationInstanceStatus(String deviceName, long jobId, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.notNull(tsdMetrics); 
        final TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedGetMitigationInfoHandler.getMitigationInstanceStatus");
        
        String tableName = mitigationInstancesTableName;

        Map<String, AttributeValue> lastEvaluatedKey = null;
        QueryResult result = null;
        Map<String, Condition> queryFilter = null;
        String indexToUse = null;
        
        Set<String> attributes = Sets.newHashSet(LOCATION_KEY, MITIGATION_STATUS_KEY, DEPLOYMENT_HISTORY_KEY);
        Map<String, Condition> keyConditions = generateKeyConditionForDeviceWorkflowId(MitigationInstancesModel.getDeviceWorkflowId(deviceName, jobId));
        
        QueryRequest queryRequest = generateQueryRequest(attributes, keyConditions, queryFilter, tableName, true, indexToUse, lastEvaluatedKey);
        try {
            result = queryMitigationsInDDB(queryRequest, subMetrics);
            lastEvaluatedKey = result.getLastEvaluatedKey();
            // If there are still more items to return then query again beginning at the point you left off before.
            while (lastEvaluatedKey != null) {
                queryRequest.withExclusiveStartKey(lastEvaluatedKey);
                QueryResult nextResult = queryMitigationsInDDB(queryRequest, subMetrics);
                result.getItems().addAll(nextResult.getItems());
                lastEvaluatedKey = result.getLastEvaluatedKey();
            }
        } catch (Exception ex) {
            String msg = "Caught Exception when querying for the mitigation instance status associated with the device workflow id of: " + 
                         MitigationInstancesModel.getDeviceWorkflowId(deviceName, jobId);
            LOG.warn(msg, ex);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addOne(NUM_ATTEMPTS_TO_GET_MITIGATION_INSTANCE_STATUSES);
            subMetrics.end();
        }
        return mitigationInstanceStatusListConverter(result.getItems());
    }
    
    /**
     * Generate the key condition for a given deviceWorkFlowid.
     * @param deviceWorkFlowId The job id of the device.
     * @return a Map of Strings as keys and Conditions as values. A key condition map.
     */
    private Map<String, Condition> generateKeyConditionForDeviceWorkflowId(String deviceWorkFlowId) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Set<AttributeValue> keyValues = new HashSet<>();
        AttributeValue keyValue = new AttributeValue(deviceWorkFlowId);
        keyValues.add(keyValue);

        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);

        keyConditions.put(DDBBasedMitigationStorageHandler.DEVICE_WORKFLOW_ID_KEY, condition);
        
        return keyConditions;
    }
    
    /**
     * Generate a List of MitigationInstanceStatuses from a List of Map of String to AttributeValue.
     * @param keyValues A List of Map of String to AttributeValue.
     * @return A List of MitigationInstanceStatuses
     */
    private List<MitigationInstanceStatus> mitigationInstanceStatusListConverter(List<Map<String, AttributeValue>> keyValues){ 
        List<MitigationInstanceStatus> listOfMitigationInstanceStatus = new ArrayList<MitigationInstanceStatus>(); 
        for (Map<String, AttributeValue> value : keyValues) {
            MitigationInstanceStatus mitigationInstanceStatus = new MitigationInstanceStatus();
            mitigationInstanceStatus.setLocation(value.get(LOCATION_KEY).getS());
            mitigationInstanceStatus.setMitigationStatus(value.get(MITIGATION_STATUS_KEY).getS());
            if (value.containsKey(DEPLOYMENT_HISTORY_KEY)) {
                List<String> deploymentHistory = new ArrayList<>(value.get(DEPLOYMENT_HISTORY_KEY).getSS());
                Collections.sort(deploymentHistory);
                mitigationInstanceStatus.setDeploymentHistory(deploymentHistory);
            }
            listOfMitigationInstanceStatus.add(mitigationInstanceStatus);
        }
        return listOfMitigationInstanceStatus;
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
    protected QueryRequest generateQueryRequest(Set<String> attributesToGet, Map<String, Condition> keyConditions,
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

}
