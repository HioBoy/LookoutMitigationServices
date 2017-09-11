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

public class DDBBasedListMitigationsHandler extends DDBBasedRequestStorageHandler implements RequestInfoHandler {
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
     * Generates a list of MitigationRequestDescription instances, where each instance wraps information about the latest request.
     * 
     * @param serviceName ServiceName for which we need to get the list of active mitigation descriptions.
     * @param deviceName DeviceName from which we need to read the list of active mitigation descriptions.
     * @param mitigationName MitigationName constraint for the active mitigations.
     * @param tsdMetrics 
     * @return List of MitigationRequestDescription instances, should contain a single request representing the latest (create/edit) mitigation request for this mitigationName.
     */
    @Override
    public List<MitigationRequestDescription> getMitigationRequestDescriptionsForMitigation(@NonNull String serviceName, @NonNull String deviceName,
                                                                                            @NonNull String mitigationName, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
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
     * @param mitigationName : mitigation name of the mitigation
     * @param exclusiveStartVersion : the start version of history retrieval, result will not include this version
     * @param maxNumberOfHistoryEntriesToFetch : the max number of history entries to retrieve
     * @param tsdMetrics : TSD metric
     * @return : list of history entries of a mitigation.
     */
    @Override
    public List<MitigationRequestDescriptionWithLocations> getMitigationHistoryForMitigation(
            String serviceName, String deviceName,
            String mitigationName, Integer exclusiveStartVersion, 
            Integer maxNumberOfHistoryEntriesToFetch, TSDMetrics tsdMetrics) {
        Validate.notEmpty(serviceName);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(mitigationName);
        Validate.notNull(maxNumberOfHistoryEntriesToFetch);
        Validate.notNull(tsdMetrics);
        QuerySpec query = new QuerySpec().withHashKey(MitigationRequestsModel.MITIGATION_NAME_KEY, mitigationName)
                .withQueryFilters(new QueryFilter(MitigationRequestsModel.SERVICE_NAME_KEY).eq(serviceName),
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

