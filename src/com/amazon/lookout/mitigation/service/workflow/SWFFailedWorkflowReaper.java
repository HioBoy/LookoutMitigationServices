package com.amazon.lookout.mitigation.service.workflow;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazon.aws158.commons.dynamo.DynamoDBHelper;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.model.DescribeWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionDetail;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.google.common.collect.Lists;

/**
 * This class is responsible for cleaning up any workflows which are marked as closed in SWF, but the DDB status shows otherwise.
 * The steps this reaper follows are:
 * 1. Query DDB Requests table to see which workflows are currently running, along with their start times.
 * 2. For each of such workflows, we also query their instances which aren't yet marked as complete.
 * 3.1 For each of such workflows, if they have a SWFRunId associated with it, we query SWF API to figure out if this workflow is still considered as running by SWF. 
 *     If it isn't so, then we check the timestamp of when SWF considered this workflow as closed and if it is above a certain threshold, we perform clean-up steps (4-5).
 * 3.2 For workflows with no SWFRunId - these could be because they're just being created and have no associated in SWF. For these, we don't query SWF, but simply check
 *     how long has it been since this request was created. If it has been above a threshold number of minutes - it implies this workflow request didn't succeed, in which 
 *     case too we perform the clean-up steps (4-5).
 * 4. For workflows to be cleaned up - we first update the instance status to Completed for the instances that were marked as incomplete.
 * 5. Only if all the instances have been updated, we go ahead and flip the workflow status to failed, to indicate that this workflow didn't shut down gracefully.
 * 
 */
@ThreadSafe
public class SWFFailedWorkflowReaper implements Runnable {
    private static final Log LOG = LogFactory.getLog(SWFFailedWorkflowReaper.class);
    
    // DDB related constants for MitigationRequests table.
    private static final String MITIGATION_REQUESTS_TABLE_NAME_PREFIX = "MITIGATION_REQUESTS_";
    private static final String MITIGATION_INSTANCES_TABLE_NAME_PREFIX = "MITIGATION_INSTANCES_";
    private static final String DEVICE_NAME_KEY = "DeviceName";
    private static final String WORKFLOW_ID_KEY = "WorkflowId";
    private static final String WORKFLOW_STATUS_KEY = "WorkflowStatus";
    private static final String RUN_ID_KEY = "RunId";
    private static final String REQUEST_DATE_IN_MILLIS = "RequestDate";
    private static final String WORKFLOW_STATUS_INDEX_NAME = "WorkflowStatus-index";
    
    // DDB related constants for MitigationInstances table.
    private static final String DEVICE_WORKFLOW_KEY = "DeviceWorkflowId";
    private static final String LOCATION_KEY = "Location";
    private static final String SCHEDULING_STATUS_KEY = "SchedulingStatus";
    
    private static final int MINUTES_TO_ALLOW_WORKFLOW_DDB_UPDATES = 3;
    private static final String SWF_WORKFLOW_FINISHED_STATUS = "CLOSED";
    
    private static final int MAX_NUM_ATTEMPTS_FOR_DDB_UPDATES = 3;
    private static final int DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    private static final int MAX_SWF_QUERY_ATTEMPTS = 3;
    private static final int SWF_RETRY_SLEEP_MILLIS_MULTIPLER = 100;
    
    private static final String DEVICE_WORKFLOW_ID_KEY_SEPARATOR = "#";
    private static final String SWF_WORKFLOW_ID_KEY_SEPARATOR = "_";
    
    private final AmazonDynamoDBClient dynamoDBClient;
    private final AmazonSimpleWorkflowClient swfClient;
    private final MetricsFactory metricsFactory;
    private final String mitigationRequestsTableName;
    private final String mitigationInstancesTableName;
    private final String swfDomain;
    
    /**
     * Private helper class to store info about a workflow.
     */
    private class WorkflowInfo {
        private final String deviceName;
        private final String workflowIdStr;
        private final String swfRunId;
        private final long requestDateInMillis;
        private final List<String> locations;
        
        public WorkflowInfo(@Nonnull String deviceName, String workflowIdStr, String swfRunId, long requestDateInMillis, 
                            @Nonnull List<String> locations) {
            Validate.notEmpty(deviceName);
            this.deviceName = deviceName;
            
            Validate.notEmpty(workflowIdStr);
            this.workflowIdStr = workflowIdStr;
            
            // SWFRunId could be null.
            this.swfRunId = swfRunId;
            
            Validate.isTrue(requestDateInMillis > 0);
            this.requestDateInMillis = requestDateInMillis;
            
            // Locations could be null, in case the individual instances have been correctly marked as complete, but the overall workflow status wasn't updated.
            this.locations = locations;
        }
        
        public String getDeviceName() {
            return deviceName;
        }
        
        public String getWorkflowIdStr() {
            return workflowIdStr;
        }
        
        public List<String> getWorkflowLocations() {
            return locations;
        }
        
        public String getSWFRunId() {
            return swfRunId;
        }
        
        public long getRequestDateInMillis() {
            return requestDateInMillis;
        }
        
        @Override
        public String toString() {
            return "WorkflowId: " + workflowIdStr + " for device: " + deviceName + " creatDateInMillis: " + requestDateInMillis + 
                   " in SWFRunId: " + swfRunId + " in locations: " + locations;
        }
    }
    
    @ConstructorProperties({"dynamoDBClient", "swfClient", "mitigationServiceDomain", "swfDomainName", "metricsFactory"})
    public SWFFailedWorkflowReaper(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull AmazonSimpleWorkflowClient swfClient, 
                                   @Nonnull String domain, @Nonnull String swfDomain, @Nonnull MetricsFactory metricsFactory) {
        Validate.notNull(dynamoDBClient);
        this.dynamoDBClient = dynamoDBClient;
        
        Validate.notNull(swfClient);
        this.swfClient = swfClient;
        
        Validate.notEmpty(domain);
        this.mitigationRequestsTableName = MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.mitigationInstancesTableName = MITIGATION_INSTANCES_TABLE_NAME_PREFIX + domain.toUpperCase();
        
        Validate.notEmpty(swfDomain);
        this.swfDomain = swfDomain;
        
        Validate.notNull(metricsFactory);
        this.metricsFactory = metricsFactory;
    }

    @Override
    public void run() {
        try {
            reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            // Simply catch all exceptions and log them as errors, to prevent this thread from dying.
            LOG.error(ex);
        }
    }
    
    /**
     * Entry point for this failed workflow reaper. Protected for unit-testing.
     * It first queries all Workflows which are still running, along with their instances that aren't marked as completed as yet. 
     * This info is wrapped in a WorkflowInfo object.
     * Next, it calls the SWF API to check if any of these workflows are indeed running, if they haven't been running for a threshold number
     * of minutes (enough for the workflow to have updated is status) then those workflows are considered as candidates for reaping.
     * Once we have the candidate workflows to reap, we set the scheduling status of all non-completed instances for this workflow to Completed,
     * while setting the overall workflow status to Failed.
     */
    protected void reapActiveDDBWorkflowsTerminatedInSWF() {
        TSDMetrics metrics = new TSDMetrics(getMetricsFactory(), "reapActiveDDBWorkflowsTerminatedInSWF");
        try {
            List<WorkflowInfo> activeDDBWorkflows = getActiveWorkflowsInfo(metrics);
            if ((activeDDBWorkflows == null) || activeDDBWorkflows.isEmpty()) {
                return;
            }
            
            List<WorkflowInfo> workflowsToReap = getSWFTerminatedWorkflowIds(activeDDBWorkflows, metrics);
            
            for (WorkflowInfo workflowToReap : workflowsToReap) {
                try {
                    setInstancesSchedulingStatusToComplete(workflowToReap, metrics);
                    setWorkflowStatusToFailed(workflowToReap, metrics);
                } catch (Exception ex) {
                    String msg = "Caught exception when updating status for workflow: " + workflowToReap;
                    LOG.error(msg, ex);
                }
            }
        } finally {
            metrics.end();
        }
    }
    
    /**
     * Responsible for querying the workflows that are still active in DDB and their corresponding instances which aren't marked as completed as yet.
     * @param metrics
     * @return List of WorkflowInfo, each WorkflowInfo instance contains details about the workflow, 
     *            including the deviceName, workflowId, corresponding swfRunId and non-completed locations.
     */
    private List<WorkflowInfo> getActiveWorkflowsInfo(TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("getActiveWorkflowsInfo");
        try {
            List<WorkflowInfo> workflowIdAndDeviceNameForActiveWorkflows = new ArrayList<>();
            
            // Query DDB for active workflows. We have a LSI on DeviceName + WorkflowStatus, hence query for each device.
            for (DeviceName deviceName : DeviceName.values()) {
                Map<String, AttributeValue> lastEvaluatedKey = null;
                do {
                    try {
                        QueryResult activeWorkflowsQueryResult = queryActiveWorkflowsForDevice(deviceName.name(), lastEvaluatedKey);
                        
                        if ((activeWorkflowsQueryResult == null) || (activeWorkflowsQueryResult.getCount() == 0)) {
                            continue;
                        }
                        
                        for (Map<String, AttributeValue> item : activeWorkflowsQueryResult.getItems()) {
                            String workflowIdStr = item.get(WORKFLOW_ID_KEY).getN();
                            
                            // SWF RunId could be null - for cases where a new request was persisted into DDB, but the SWF RunId was not yet updated.
                            String swfRunId = null;
                            if (item.containsKey(RUN_ID_KEY)) {
                                swfRunId = item.get(RUN_ID_KEY).getS();
                            }
                            
                            long requestDateInMillis = Long.parseLong(item.get(REQUEST_DATE_IN_MILLIS).getN());
                            
                            List<String> locations = queryNonCompletedInstancesForWorkflow(deviceName.name(), workflowIdStr);
                            WorkflowInfo workflowInfo = new WorkflowInfo(deviceName.name(), workflowIdStr, swfRunId, requestDateInMillis, locations);
                            workflowIdAndDeviceNameForActiveWorkflows.add(workflowInfo);
                        }
                        lastEvaluatedKey = activeWorkflowsQueryResult.getLastEvaluatedKey();
                    } catch (Exception ex) {
                        // Handle any exceptions by logging a warning and moving on to the next device.
                        String msg = "Caught exception when querying active workflows for device: " + deviceName.name();
                        LOG.warn(msg, ex);
                        break; // break out of the do-while loop.
                    }
                } while (lastEvaluatedKey != null);
            }
            
            return workflowIdAndDeviceNameForActiveWorkflows;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for querying SWF to figure out if a workflow is still running or is considered closed by SWF. Protected for unit-testing.
     * @param workflowsActiveInDDB Workflows currently deemed active based on their status in DDB.
     * @param metrics
     * @return List of WorkflowInfo where each instance represents info for the Workflow which reflects an active status in DDB but is considered closed by SWF.
     */
    protected List<WorkflowInfo> getSWFTerminatedWorkflowIds(List<WorkflowInfo> workflowsActiveInDDB, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("getSWFTerminatedWorkflowIds");
        try {
            List<WorkflowInfo> terminatedWorkflowIds = new ArrayList<>();
            DateTime now = new DateTime(DateTimeZone.UTC);
            
            for (WorkflowInfo workflowInfo : workflowsActiveInDDB) {
                String swfRunId = workflowInfo.getSWFRunId();
                String workflowIdStr = workflowInfo.getWorkflowIdStr();
                String deviceName = workflowInfo.getDeviceName();
                long workflowRequestDateInMillis = workflowInfo.getRequestDateInMillis();
                
                // If there is no SWFRunId associated with the workflow, then that means the workflow was never started in SWF.
                // In such cases, we check if it has been beyond threshold number of minutes (which is way higher than what it would normally take the code to update its DDB status).
                // If it is beyond threshold number of minutes, we treat this workflow as one to be reaped.
                if (swfRunId == null) {
                    if (now.minusMinutes(MINUTES_TO_ALLOW_WORKFLOW_DDB_UPDATES).isAfter(workflowRequestDateInMillis)) {
                        terminatedWorkflowIds.add(workflowInfo);
                    }
                } else {
                    // Query SWF to see if this workflow is currently running.
                    try {
                        if (isWorkflowClosedInSWF(deviceName, workflowIdStr, swfRunId, subMetrics)) {
                            terminatedWorkflowIds.add(workflowInfo);
                        }
                    } catch (Exception ex) {
                        String msg = "Unable to query executionStatus for workflow: " + workflowIdStr + ", deviceName: " + deviceName +
                                     " with SWFRunId: " + swfRunId + ", continuing evaluating other workflows.";
                        LOG.error(msg, ex);
                    }
                }
            }
            return terminatedWorkflowIds;
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Responsible for setting the scheduling status to complete for mitigation instances corresponding to the workflows that should be cleaned up.
     * @param workflowToReap Workflows that need to be cleaned up.
     * @param metrics
     */
    private void setInstancesSchedulingStatusToComplete(WorkflowInfo workflowToReap, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("setInstancesSchedulingStatusToComplete");
        try {
            String deviceName = workflowToReap.getDeviceName();
            String workflowIdStr = workflowToReap.getWorkflowIdStr();
            String deviceWorkflowIdKey = deviceName + DEVICE_WORKFLOW_ID_KEY_SEPARATOR + workflowIdStr;
            
            // Update all locations to have their scheduling status marked as Completed.
            for (String location : workflowToReap.getWorkflowLocations()) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put(DEVICE_WORKFLOW_KEY, new AttributeValue(deviceWorkflowIdKey));
                key.put(LOCATION_KEY, new AttributeValue(location));
                
                Map<String, AttributeValueUpdate> updates = new HashMap<>();
                AttributeValueUpdate schedulingStatusUpdate = new AttributeValueUpdate(new AttributeValue(SchedulingStatus.COMPLETED.name()), AttributeAction.PUT);
                updates.put(SCHEDULING_STATUS_KEY, schedulingStatusUpdate);
                
                updateStatusInDDB(mitigationInstancesTableName, key, updates, null, subMetrics);
            }
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for setting the workflow status to failed for workflows that should be cleaned up.
     * @param workflowToReap Workflows that need to be cleaned up.
     * @param metrics
     */
    private void setWorkflowStatusToFailed(WorkflowInfo workflowToReap, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("setWorkflowStatusToFailed");
        try {
            String deviceName = workflowToReap.getDeviceName();
            String workflowIdStr = workflowToReap.getWorkflowIdStr();
            
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(DEVICE_NAME_KEY, new AttributeValue(deviceName));
            key.put(WORKFLOW_ID_KEY, new AttributeValue().withN(workflowIdStr));
            
            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            AttributeValueUpdate workflowStatusUpdate = new AttributeValueUpdate(new AttributeValue(WorkflowStatus.FAILED), AttributeAction.PUT);
            updates.put(WORKFLOW_STATUS_KEY, workflowStatusUpdate);
            
            Map<String, ExpectedAttributeValue> expectations = new HashMap<>();
            ExpectedAttributeValue workflowStatusExpectation = new ExpectedAttributeValue(new AttributeValue(WorkflowStatus.RUNNING));
            workflowStatusExpectation.setExists(true);
            expectations.put(WORKFLOW_STATUS_KEY, workflowStatusExpectation);
            
            updateStatusInDDB(mitigationRequestsTableName, key, updates, expectations, subMetrics);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for querying DDB to find workflows whose status reflects that they are currently active.
     * @param deviceName DeviceName whose workflows need to be queried.
     * @param lastEvaluatedKey Represents the lastEvaluatedKey returned by DDB for the previous key, null if this is the first query.
     * @return QueryResult containing the result of querying for workflows whose status reflect them as being active.
     */
    private QueryResult queryActiveWorkflowsForDevice(String deviceName, Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest request = new QueryRequest(mitigationRequestsTableName);
        Map<String, Condition> queryConditions = new HashMap<>();
        List<AttributeValue> conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue(deviceName));
        queryConditions.put(DEVICE_NAME_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
        
        conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue(WorkflowStatus.RUNNING));
        queryConditions.put(WORKFLOW_STATUS_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
        request.setKeyConditions(queryConditions);
        
        request.setIndexName(WORKFLOW_STATUS_INDEX_NAME);
        request.setConsistentRead(true);
        request.setAttributesToGet(Lists.newArrayList(WORKFLOW_ID_KEY, RUN_ID_KEY, REQUEST_DATE_IN_MILLIS));
        if (lastEvaluatedKey != null) {
            request.setExclusiveStartKey(lastEvaluatedKey);
        }
        
        return queryDynamoDB(request);
    }
    
    /**
     * Responsible for querying the mitigation instances which aren't marked as complete, for the workflow represented by the input parameters. Protected for unit tests.
     * @param deviceName DeviceName for the workflow whose instances are being queried.
     * @param workflowIdStr WorkflowId represented as string, whose instances are being queried. 
     * @return List of String, where each entry represents the location (instance) which is not marked as complete for the workflow represented by the input parameters.
     */
    protected List<String> queryNonCompletedInstancesForWorkflow(String deviceName, String workflowIdStr) {
        List<String> nonCompletedWorkflowInstances = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            QueryRequest request = new QueryRequest(mitigationInstancesTableName);
            String deviceWorkflowKey = deviceName + "-" + workflowIdStr;
            
            Map<String, Condition> queryConditions = new HashMap<>();
            List<AttributeValue> conditionAttributes = new ArrayList<>();
            conditionAttributes.add(new AttributeValue(deviceWorkflowKey));
            queryConditions.put(DEVICE_WORKFLOW_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
            request.setKeyConditions(queryConditions);
            
            request.setConsistentRead(true);
            request.setAttributesToGet(Lists.newArrayList(SCHEDULING_STATUS_KEY, LOCATION_KEY));
            if (lastEvaluatedKey != null) {
                request.setExclusiveStartKey(lastEvaluatedKey);
            }
            
            QueryResult result = queryDynamoDB(request);
            if ((result == null) || (result.getCount() == 0)) {
                continue;
            }
            
            lastEvaluatedKey = result.getLastEvaluatedKey();
            
            for (Map<String, AttributeValue> item : result.getItems()) {
                String schedulingStatus = item.get(SCHEDULING_STATUS_KEY).getS();
                if (schedulingStatus.equals(SchedulingStatus.COMPLETED.name())) {
                    continue;
                }
                
                nonCompletedWorkflowInstances.add(item.get(LOCATION_KEY).getS());
            }
        } while (lastEvaluatedKey != null);
        
        return nonCompletedWorkflowInstances;
    }
    
    /**
     * Responsible for updating status field in DDB. Protected for unit-testing.
     * @param tableName Table where we want to update the status field.
     * @param key Key corresponding to the item which needs an update to the status field.
     * @param updates Map representing the updates that need to be done.
     * @param expectations Map representing the expected values to check against before performing the update.
     * @param metrics
     */
    protected void updateStatusInDDB(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> updates,
                                     Map<String, ExpectedAttributeValue> expectations, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("updateStatusInDDB");
        
        int numAttempts = 0;
        try {
            // Attempt to update DDB for a fixed number of times.
            while (numAttempts++ < MAX_NUM_ATTEMPTS_FOR_DDB_UPDATES) {
                try {
                    DynamoDBHelper.updateItem(dynamoDBClient, tableName, updates, key, expectations, null, null, null);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    String msg = "Caught a ConditionalCheckFailedException when attempting to update with key: " + key + ", updates: " + updates +
                                 ", expectations: " + expectations + " on table: " + tableName;
                    LOG.warn(msg, ex);
                    throw new RuntimeException(msg, ex);
                } catch (Exception ex) {
                    String msg = "Caught exception when attempting to update with key: " + key + ", updates: " + updates +
                                     ", expectations: " + expectations + " on table: " + tableName;
                    LOG.warn(msg, ex);
        
                   if (numAttempts < MAX_NUM_ATTEMPTS_FOR_DDB_UPDATES) {
                       try {
                           Thread.sleep(DDB_UPDATE_ITEM_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                       } catch (InterruptedException ignored) {}
                   }
                }
            }
            String msg = "Unable to update status in DDB table: " + tableName + " for key: " + key + ", attributeToUpdate: " + updates + " expectations: " + expectations;
            throw new RuntimeException(msg);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for querying SWF API to identify if the workflow run represented by the input parameters is considered Closed by SWF. Protected to allow unit-tests.
     * @param deviceName Device corresponding to the workflow run whose SWF status is being queried.
     * @param workflowIdStr WorkflowId represented as String
     * @param swfRunId RunId created by SWF to identify an instance of SWF workflow.
     * @param metrics
     * @return boolean flag indicating true if the workflow represented by the input parameters is considered Closed by SWF, false otherwise. 
     */
    protected boolean isWorkflowClosedInSWF(String deviceName, String workflowIdStr, String swfRunId, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("isWorkflowClosedInSWF");
        try {
            int numAttempts = 0;
            while (numAttempts++ < MAX_SWF_QUERY_ATTEMPTS) {
                try {
                    DateTime now = new DateTime(DateTimeZone.UTC);
                    String swfWorkflowId = deviceName + SWF_WORKFLOW_ID_KEY_SEPARATOR + workflowIdStr;
                    WorkflowExecution workflowExecution = new WorkflowExecution().withRunId(swfRunId).withWorkflowId(swfWorkflowId);
                    
                    DescribeWorkflowExecutionRequest request = new DescribeWorkflowExecutionRequest();
                    request.setDomain(swfDomain);
                    request.setExecution(workflowExecution);
                    
                    WorkflowExecutionDetail workflowDetail = swfClient.describeWorkflowExecution(request);
                    WorkflowExecutionInfo workflowExecutionInfo = workflowDetail.getExecutionInfo();
                    String executionStatus = workflowExecutionInfo.getExecutionStatus();
                    if (executionStatus.equals(SWF_WORKFLOW_FINISHED_STATUS)) {
                        // If SWF has marked this workflow as being finished, check how long ago did this happen, giving the
                        // deciders some time to perform the appropriate DDB updates.
                        DateTime closeTimestamp = new DateTime(workflowExecutionInfo.getCloseTimestamp());
                        if (now.minusMinutes(MINUTES_TO_ALLOW_WORKFLOW_DDB_UPDATES).isAfter(closeTimestamp)) {
                            return true;
                        }
                    }
                    return false;
                } catch (Exception ex) {
                    String msg = "Caught exception when querying status in SWF for workflow: " + workflowIdStr + ", deviceName: " + deviceName +
                                 " with SWFRunId: " + swfRunId;
                    LOG.warn(msg, ex);
                    
                    if (numAttempts < MAX_SWF_QUERY_ATTEMPTS) {
                        try {
                            Thread.sleep(SWF_RETRY_SLEEP_MILLIS_MULTIPLER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            String msg = "Unable to query status in SWF for workflow: " + workflowIdStr + ", deviceName: " + deviceName +
                         " with SWFRunId: " + swfRunId;
            throw new RuntimeException(msg);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper to query DynamoDB, delegates the call to DynamoDBHelper. Protected to allow for unit-testing.
     * @param request QueryRequest to be issued to DynamoDB.
     * @return QueryResult representing the result of running the query passed above against DynamoDB.
     */
    protected QueryResult queryDynamoDB(QueryRequest request) {
        return DynamoDBHelper.queryItemAttributesFromTable(dynamoDBClient, request);
    }
    
    /**
     * Helper to return the metrics factory. Protected for unit-testing.
     */
    protected MetricsFactory getMetricsFactory() {
        return metricsFactory;
    }
}
