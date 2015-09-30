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
import org.joda.time.DateTime;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.MissingLocationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Sets;

import static com.amazon.lookout.ddb.model.MitigationInstancesModel.*;

public class DDBBasedGetMitigationInfoHandler extends DDBBasedMitigationStorageHandler implements MitigationInstanceInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedGetMitigationInfoHandler.class);
    
    // Keys for TSDMetric properties.
    private static final String NUM_ATTEMPTS_TO_GET_MITIGATION_INSTANCE_STATUSES = "NumGetMitigationInstanceStatusesAttempts";
    private final DynamoDB dynamoDB;
    
    public DDBBasedGetMitigationInfoHandler(AmazonDynamoDBClient dynamoDBClient, String domain) {
        super(dynamoDBClient, domain);
        this.dynamoDB = new DynamoDB(dynamoDBClient);
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
            subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
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

    /**
     * Get mitigation deployment instance on a location.
     * For example, if a location has deployment jobId : 101, 112, 134, 135, 136
     * The return order will be 136, 135, 134, 112, 101.
     * This API query GSI, so result is eventually consistent.
     * @param deviceName : device name
     * @param location : location
     * @param maxNumberOfHistoryEntriesToFetch : max number of entry to retrieve
     * @param exclusiveLastEvaluatedTimestamp : fetch the deployment history that is before to this timestamp in UTC, 
     *          result will not include this timestamp
     * @param tsdMetrics : TSD metric
     * @return : return list of mitigation instance status
     * @throws : MissingLocationException400, if location is not found
     */
    @Override
    public List<LocationDeploymentInfo> getLocationDeploymentInfoOnLocation(String deviceName, String location,
            int maxNumberOfHistoryEntriesToFetch, Long exclusiveLastEvaluatedTimestamp, TSDMetrics tsdMetrics) {
        Validate.notEmpty(deviceName);
        Validate.notEmpty(location);
        Validate.notNull(tsdMetrics); 
        Validate.isTrue(maxNumberOfHistoryEntriesToFetch > 0);

        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics(
                "DDBBasedGetMitigationInfoHandler.getLocationDeploymentInfoOnLocation")) {
            QuerySpec query = new QuerySpec().withHashKey(LOCATION_RANGE_KEY, location)
                    .withAttributesToGet(CREATE_DATE_KEY, DEPLOYMENT_HISTORY_KEY,
                            DEVICE_WORKFLOW_ID_HASH_KEY, LOCATION_RANGE_KEY, MITIGATION_NAME_KEY,
                            MITIGATION_STATUS_KEY, MITIGATION_VERSION_KEY, SCHEDULING_STATUS_KEY)
                            .withQueryFilters(new QueryFilter(DEVICE_NAME_KEY).eq(deviceName))
                            .withConsistentRead(false).withMaxResultSize(maxNumberOfHistoryEntriesToFetch * 10)
                            .withScanIndexForward(false);
            if (exclusiveLastEvaluatedTimestamp != null) {
                query.withRangeKeyCondition(new RangeKeyCondition(CREATE_DATE_KEY).lt(
                        new DateTime(exclusiveLastEvaluatedTimestamp).toString(CREATE_DATE_FORMATTER)));
            }

            List<LocationDeploymentInfo> listOfLocationDeploymentInfo = new ArrayList<>();
            try {
                for (Item item : dynamoDB.getTable(mitigationInstancesTableName)
                        .getIndex(LOCATION_CREATE_DATE_GSI).query(query)) {
                    listOfLocationDeploymentInfo.add(convertLocationDeploymentInfo(item));
                    if (listOfLocationDeploymentInfo.size() >= maxNumberOfHistoryEntriesToFetch) {
                        break;
                    }
                }
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when querying for the mitigation instance status "
                        + "associated with location : %s, and device name : %s", location, deviceName);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }

            if (!listOfLocationDeploymentInfo.isEmpty()) {
                return listOfLocationDeploymentInfo;
            }
            throw new MissingLocationException400("Can not find any deployment record at location " + location
                    + " on device " + deviceName);
        }
    }
    
    private LocationDeploymentInfo convertLocationDeploymentInfo(Item item) {
        LocationDeploymentInfo info = new LocationDeploymentInfo();
        info.setDeployDate(item.getString(CREATE_DATE_KEY));
        Set<String> deploymentHistory = item.getStringSet(DEPLOYMENT_HISTORY_KEY);
        if (deploymentHistory != null) {
            info.setDeploymentHistory(new ArrayList<String>(deploymentHistory));
        }
        info.setJobId(getWorkflowId(item.getString(DEVICE_WORKFLOW_ID_HASH_KEY)));
        info.setMitigationName(item.getString(MITIGATION_NAME_KEY));
        info.setMitigationStatus(item.getString(MITIGATION_STATUS_KEY));
        if (item.isPresent(MITIGATION_VERSION_KEY) && !item.isNull(MITIGATION_VERSION_KEY)) {
            info.setMitigationVersion(item.getInt(MITIGATION_VERSION_KEY));
        }
        info.setSchedulingStatus(item.getString(SCHEDULING_STATUS_KEY));
        return info;
    }
}
