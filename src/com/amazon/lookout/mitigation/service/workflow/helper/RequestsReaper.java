package com.amazon.lookout.mitigation.service.workflow.helper;

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
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.workflow.model.RequestToReap;
import com.amazon.lookout.workflow.reaper.RequestReaperConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.ExecutionStatus;
import com.amazonaws.services.simpleworkflow.model.ListClosedWorkflowExecutionsRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionAlreadyStartedException;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionFilter;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfos;
import com.google.common.collect.Lists;

/**
 * This class is responsible for cleaning up any workflows whose state needs to be brought in sync with the reality on the mitigation devices.
 * The steps this reaper follows are:
 *  1. Query MITIGATION_REQUESTS to find workflows whose WorkflowStatus is not in {SUCCEEDED} and "Reaped" flag not set to "true"
 *  2. For such entries, if a request has WorkflowStatus = RUNNING and has RequestDate > N seconds, then query SWF to ensure if it is still actively running,
 *     if it is, then we skip any further work on such requests.
 *  3. For each request to process, query MITIGATION_INSTANCES for the corresponding instances. 
 *  4. For each request, only keep track of the instances from above which satisfy one or more of the following conditions:
 *     4.1 ActiveMitigationsUpdated flag is either null or set to false.
 *     4.2 SchedulingStatus is not yet set to COMPLETED.
 *     4.3 MitigationStatus isn't one of the statuses that is considered as the final state (eg: DEPLOY_COMPLETED, POST_DEPLOYMENT_CHECKS_PASSED).
 *  5. For each of such requests, start a separate workflow, with the workflowId set to <OriginalWorkflowId>_Reaper - to ensure only one of such workflows is ever running.
 *  
 */
@ThreadSafe
public class RequestsReaper implements Runnable {
    private static final Log LOG = LogFactory.getLog(RequestsReaper.class);
    
    // A buffer of number of seconds for the MitigationService activity to start a workflow.
    private static final int BUFFER_SECONDS_BEFORE_STARTING_WORKFLOW = 10;
    
    // Number of seconds to allow workflow to perform DDB updates once the workflow has completed.
    private static final int SECONDS_TO_ALLOW_WORKFLOW_DDB_UPDATES = 60;
    
    private static final int MAX_SWF_QUERY_ATTEMPTS = 3;
    private static final int SWF_RETRY_SLEEP_MILLIS_MULTIPLER = 100;
    
    private static final String SWF_WORKFLOW_ID_KEY_SEPARATOR = "_";
    
    // Keys to be used for publishing PMET metrics.
    private static final String NUM_REQUESTS_TO_REAP_METRIC_KEY = "NumRequestsToReap";
    private static final String FAILED_TO_START_WORKFLOW_METRIC_KEY = "FailedToStartWorkflow";
    private static final String NUM_REAPER_WORKFLOWS_STARTED_METRIC_KEY = "NumReaperWorkflows";
    private static final String PRE_EXISTING_REAPER_WORKFLOW_METRIC_KEY = "NumPreExistingReaperWorkflows";
    
    private final AmazonDynamoDBClient dynamoDBClient;
    private final AmazonSimpleWorkflowClient swfClient;
    private final MetricsFactory metricsFactory;
    private final String mitigationRequestsTableName;
    private final String mitigationInstancesTableName;
    private final String swfDomain;
    private final int maxSecondsToStartWorkflow;
    private final SWFWorkflowStarter workflowStarter;
    
    @ConstructorProperties({"dynamoDBClient", "swfClient", "swfDomain", "swfDomainName", "swfSocketTimeoutSeconds", "swfConnTimeoutSeconds", "workflowStarter", "metricsFactory"})
    public RequestsReaper(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull AmazonSimpleWorkflowClient swfClient, @Nonnull String domain, @Nonnull String swfDomain, 
                          int swfSocketTimeoutSeconds, int swfConnTimeoutSeconds, @Nonnull SWFWorkflowStarter workflowStarter, @Nonnull MetricsFactory metricsFactory) {
        Validate.notNull(dynamoDBClient);
        this.dynamoDBClient = dynamoDBClient;
        
        Validate.notNull(swfClient);
        this.swfClient = swfClient;
        
        Validate.notEmpty(domain);
        this.mitigationRequestsTableName = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase();
        this.mitigationInstancesTableName = MitigationInstancesModel.MITIGATION_INSTANCES_TABLE_NAME_PREFIX + domain.toUpperCase();
        
        Validate.notEmpty(swfDomain);
        this.swfDomain = swfDomain;
        
        Validate.isTrue(swfConnTimeoutSeconds > 0);
        Validate.isTrue(swfSocketTimeoutSeconds > 0);
        maxSecondsToStartWorkflow = swfConnTimeoutSeconds + swfSocketTimeoutSeconds + BUFFER_SECONDS_BEFORE_STARTING_WORKFLOW;
        
        Validate.notNull(workflowStarter);
        this.workflowStarter = workflowStarter;
        
        Validate.notNull(metricsFactory);
        this.metricsFactory = metricsFactory;
    }

    @Override
    public void run() {
        TSDMetrics metrics = new TSDMetrics(getMetricsFactory(), "RunRequestsReaper");
        try {
            reapRequests(metrics);
        } catch (Exception ex) {
            // Simply catch all exceptions and log them as errors, to prevent this thread from dying.
            LOG.error(ex);
        } finally {
            metrics.end();
        }
    }
    
    /**
     * Entry point for this workflow reaper. Protected for unit-testing.
     * It first queries all Requests to identify ones that need to be reaped, along with the locations that correspond to that request which needs updating.
     * Each of one such requests to be reaped is wrapped in a RequestToReap object.
     * It next starts a workflow per RequestToReap instance, setting the workflowId as the RequestDeviceName + <SEPARATOR> OriginalWorkflowId + <SEPARATOR> + Reaper.
     * This ensures only one of such workflow would be running at any point in time.
     */
    protected void reapRequests(@Nonnull TSDMetrics tsdMetrics) {
        TSDMetrics metrics = tsdMetrics.newSubMetrics("reapRequests");
        try {
            metrics.addCount(NUM_REQUESTS_TO_REAP_METRIC_KEY, 0);
            metrics.addCount(FAILED_TO_START_WORKFLOW_METRIC_KEY, 0);
            metrics.addCount(NUM_REAPER_WORKFLOWS_STARTED_METRIC_KEY, 0);
            metrics.addCount(PRE_EXISTING_REAPER_WORKFLOW_METRIC_KEY, 0);
            
            List<RequestToReap> activeDDBWorkflows = getRequestsToReap(metrics);
            if ((activeDDBWorkflows == null) || activeDDBWorkflows.isEmpty()) {
                return;
            }
            metrics.addCount(NUM_REQUESTS_TO_REAP_METRIC_KEY, activeDDBWorkflows.size());
            
            for (RequestToReap requestToReap : activeDDBWorkflows) {
                String deviceName = requestToReap.getDeviceName();
                String workflowIdStr = requestToReap.getWorkflowIdStr();
                String swfWorkflowId = deviceName + SWF_WORKFLOW_ID_KEY_SEPARATOR + workflowIdStr + SWF_WORKFLOW_ID_KEY_SEPARATOR + "Reaper";
                WorkflowClientExternal workflowClient = getWorkflowStarter().createReaperWorkflowClient(swfWorkflowId, metrics);
                
                try {
                    getWorkflowStarter().startReaperWorkflow(swfWorkflowId, requestToReap, workflowClient, metrics);
                    metrics.addOne(NUM_REAPER_WORKFLOWS_STARTED_METRIC_KEY);
                    
                    String swfRunId = workflowClient.getWorkflowExecution().getRunId();
                    LOG.info("Started reaper workflow for workflowId: " + workflowIdStr + " for device: " + deviceName + 
                             " with swfWorkflowId: " + swfWorkflowId + " and swfRunId: " + swfRunId);
                } catch (WorkflowExecutionAlreadyStartedException ex) {
                    LOG.info("Reaper workflow for workflowId: " + workflowIdStr + " for device: " + deviceName + " with swfWorkflowId: " + swfWorkflowId + " is already running.", ex);
                    metrics.addOne(PRE_EXISTING_REAPER_WORKFLOW_METRIC_KEY);
                } catch (Exception ex) {
                    LOG.error("Caught exception starting reaper workflow for workflowId: " + workflowIdStr + " for device: " + deviceName + " with swfWorkflowId: " + swfWorkflowId, ex);
                    metrics.addOne(FAILED_TO_START_WORKFLOW_METRIC_KEY);
                }
            }
        } finally {
            metrics.end();
        }
    }
    
    /**
     * Responsible for getting the user requests which need to be reaped. Requests which need to be is determined using the following algorithm:
     *  1. Query MITIGATION_REQUESTS to find workflows whose WorkflowStatus is not in {SUCCEEDED} and "Reaped" flag not set to "true"
     *  2. For such entries, if a request has WorkflowStatus = RUNNING and has RequestDate > N seconds, then query SWF to ensure if it is still actively running,
     *     if it is, then we skip any further work on such requests.
     *  3. For each request to process, query MITIGATION_INSTANCES for the corresponding instances. 
     *  4. For each request, only keep track of the instances from above which satisfy one or more of the following conditions:
     *     4.1 ActiveMitigationsUpdated flag is either null or set to false.
     *     4.2 SchedulingStatus is not yet set to COMPLETED.
     *     4.3 MitigationStatus isn't one of the statuses that is considered as the final state (eg: DEPLOY_COMPLETED, POST_DEPLOYMENT_CHECKS_PASSED).
     * Method has protected visibility for unit-testing.
     * @param metrics
     * @return List of RequestToReap, each RequestToReap instance contains details about the user request that needs to be reaped, along with the locations
     *         corresponding to that request which aren't in a clean state.
     */
    protected List<RequestToReap> getRequestsToReap(@Nonnull TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("getWorkflowsToReap");
        try {
            subMetrics.addCount("NumDevicesFailingReaperCheck", 0);
            
            List<RequestToReap> requestsToBeReaped = new ArrayList<>();
            
            // Query DDB for active workflows. We have a LSI on DeviceName + UpdateWorkflowId, hence query for each device.
            for (DeviceName deviceName : DeviceName.values()) {
                Map<String, AttributeValue> lastEvaluatedKey = null;
                do {
                    try {
                        // Get a list of active requests which weren't successful and haven't been reaped as yet.
                        QueryResult queryResult = getUnsuccessfulUnreapedRequests(deviceName.name(), lastEvaluatedKey);
                        if ((queryResult == null) || (queryResult.getCount() == 0)) {
                            continue;
                        }
                        
                        for (Map<String, AttributeValue> item : queryResult.getItems()) {
                            String workflowIdStr = item.get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN();
                            List<String> locations = item.get(MitigationRequestsModel.LOCATIONS_KEY).getSS();
                            String workflowStatus = item.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY).getS();
                            long requestDateInMillis = Long.parseLong(item.get(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY).getN());
                            String mitigationName = item.get(MitigationRequestsModel.MITIGATION_NAME_KEY).getS();
                            int mitigationVersion = Integer.parseInt(item.get(MitigationRequestsModel.MITIGATION_VERSION_KEY).getN());
                            String mitigationTemplate = item.get(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY).getS();
                            String deviceScope = item.get(MitigationRequestsModel.DEVICE_SCOPE_KEY).getS();
                            String requestType = item.get(MitigationRequestsModel.REQUEST_TYPE_KEY).getS();
                            String serviceName = item.get(MitigationRequestsModel.SERVICE_NAME_KEY).getS();
                            
                            // SWF RunId could be null - for cases where a new request was persisted into DDB, but the SWF RunId was not yet updated.
                            String swfRunId = null;
                            if (item.containsKey(MitigationRequestsModel.RUN_ID_KEY)) {
                                swfRunId = item.get(MitigationRequestsModel.RUN_ID_KEY).getS();
                            }
                            
                            // If the workflow status is Running, check if it has been running for at least N seconds, if not, then skip this entry.
                            if (workflowStatus.equals(WorkflowStatus.RUNNING)) {
                                DateTime now = new DateTime(DateTimeZone.UTC);
                                if (now.minusSeconds(getMaxSecondsToStartWorkflow()).isBefore(requestDateInMillis)) {
                                    continue;
                                }
                                
                                // Query SWF to see if this workflow is still active or not. If it is still active, don't perform any further steps.
                                if (!isWorkflowClosedInSWF(deviceName.name(), workflowIdStr, subMetrics)) {
                                    continue;
                                }
                            }
                            
                            // Get instances for the workflow corresponding to the request being evaluated.
                            Map<String, Map<String, AttributeValue>> instancesDetails = queryInstancesForWorkflow(deviceName.name(), workflowIdStr);
                            
                            // Filter only the instances that need some state to be corrected.
                            Map<String, Map<String, AttributeValue>> instancesToBeReaped = filterInstancesToBeReaped(locations, instancesDetails);
                            
                            // Wrap up this request and its instances in a RequestToReap object. 
                            RequestToReap workflowInfo = new RequestToReap(workflowIdStr, swfRunId, deviceName.name(), deviceScope, serviceName, mitigationName, 
                                                                           mitigationVersion, mitigationTemplate, requestType, requestDateInMillis, instancesToBeReaped);
                            requestsToBeReaped.add(workflowInfo);
                        }
                        lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                    } catch (Exception ex) {
                        // Handle any exceptions by logging a warning and moving on to the next device.
                        String msg = "Caught exception when querying active workflows for device: " + deviceName.name();
                        LOG.warn(msg, ex);
                        subMetrics.addOne("NumDevicesFailingReaperCheck");
                        break; // break out of the do-while loop.
                    }
                } while (lastEvaluatedKey != null);
            }
            return requestsToBeReaped;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Filter instances that need to be reaped, ignoring all other instances that need no additional checks. Protected for unit-testing.
     * @param locations List of locations where this request was originally intended to run.
     * @param instancesDetails Map whose key is location and value is an attribute-value Map for representing the state of the location in the key.
     * @return Map whose key is the locations that needs to be reaped and whose value is an attribute-value Map for representing the state of the location.
     */
    protected Map<String, Map<String, AttributeValue>> filterInstancesToBeReaped(List<String> locations, Map<String, Map<String, AttributeValue>> instancesDetails) {
        Map<String, Map<String, AttributeValue>> filteredInstances = new HashMap<>();
        
        for (String location : locations) {
            // If we don't find any entry in the MITIGATION_INSTANCES table for a location where this request was run, then it implies the workflow never created
            // the entry for that location. Hence simply adding a null for the current state of that location since no action would have been taken at that location for the request.
            if (!instancesDetails.containsKey(location)) {
                filteredInstances.put(location, null);
                continue;
            }
            
            Map<String, AttributeValue> details = instancesDetails.get(location);
            
            // If the ActiveMitigationsUpdated is null or set to false, the reaper needs to fix the state in the active mitigations table, hence keeping track of this instance. 
            String activeMitigationsUpdated = null;
            if (details.containsKey(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY)) {
                activeMitigationsUpdated = details.get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY).getS();
            }
            
            if ((activeMitigationsUpdated == null) || activeMitigationsUpdated.equals("false")) {
                filteredInstances.put(location, details);
                continue;
            }
            
            // If the workflow has ended without having this instance's scheduling status set to Completed, then we might potentially
            // be blocking other workflows. Hence keep track of this instance for performing reaper tasks.
            String schedulingStatus = details.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY).getS();
            if (!schedulingStatus.equals(SchedulingStatus.COMPLETED.name())) {
                filteredInstances.put(location, details);
                continue;
            }
            
            // If the workflow has ended with this instance's status not reflecting a state indicating it finished all of its operations,
            // we need to keep track of it for performing reaper tasks.
            String mitigationStatus = details.get(MitigationInstancesModel.MITIGATION_STATUS_KEY).getS();
            if (!RequestReaperConstants.COMPLETED_MITIGATION_STATUSES.contains(mitigationStatus)) {
                filteredInstances.put(location, details);
                continue;
            }
        }
        
        return filteredInstances;
    }
    
    /**
     * Responsible for querying DDB to find requests which weren't completely successful and haven't yet been reaped.
     * @param deviceName DeviceName whose workflow requests need to be queried.
     * @param lastEvaluatedKey Represents the lastEvaluatedKey returned by DDB for the previous key, null if this is the first query.
     * @return QueryResult containing the result of querying for workflows whose status reflect them as being active.
     */
    protected QueryResult getUnsuccessfulUnreapedRequests(@Nonnull String deviceName, @Nonnull Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest request = createQueryForRequests(deviceName, lastEvaluatedKey);
        return queryDynamoDB(request);
    }
    
    /**
     * Helper to create the QueryRequest to be issued to DDB for getting the list of requests which weren't completely successful and haven't yet been reaped.
     * @param deviceName DeviceName whose workflow requests need to be queried.
     * @param lastEvaluatedKey Represents the lastEvaluatedKey returned by DDB for the previous key, null if this is the first query.
     * @return QueryRequest representing the query to be issued against DDB to get the appropriate list of requests that need to be reaped.
     */
    protected QueryRequest createQueryForRequests(@Nonnull String deviceName, @Nonnull Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest request = new QueryRequest(getRequestsTableName());
        Map<String, Condition> queryConditions = new HashMap<>();
        
        List<AttributeValue> conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue(deviceName));
        queryConditions.put(MitigationRequestsModel.DEVICE_NAME_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
        
        conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue().withN("0"));
        queryConditions.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
        request.setKeyConditions(queryConditions);
        
        request.setIndexName(MitigationRequestsModel.UNEDITED_MITIGATIONS_LSI_NAME);
        request.setConsistentRead(true);
        request.setAttributesToGet(Lists.newArrayList(MitigationRequestsModel.WORKFLOW_ID_KEY, MitigationRequestsModel.RUN_ID_KEY, 
                                                      MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, MitigationRequestsModel.WORKFLOW_STATUS_KEY, 
                                                      MitigationRequestsModel.DEVICE_SCOPE_KEY, MitigationRequestsModel.LOCATIONS_KEY, 
                                                      MitigationRequestsModel.MITIGATION_NAME_KEY, MitigationRequestsModel.MITIGATION_VERSION_KEY, 
                                                      MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, MitigationRequestsModel.REQUEST_TYPE_KEY,
                                                      MitigationRequestsModel.SERVICE_NAME_KEY));
        if (lastEvaluatedKey != null) {
            request.setExclusiveStartKey(lastEvaluatedKey);
        }
        
        Map<String, Condition> queryFilters = new HashMap<>();
        AttributeValue workflowStatusValue = new AttributeValue(WorkflowStatus.SUCCEEDED);
        Condition workflowStatusCondition = new Condition().withAttributeValueList(workflowStatusValue).withComparisonOperator(ComparisonOperator.NE);
        queryFilters.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, workflowStatusCondition);
        
        AttributeValue reapedValue = new AttributeValue("true");
        Condition reapedCondition = new Condition().withAttributeValueList(reapedValue).withComparisonOperator(ComparisonOperator.NE);
        queryFilters.put(MitigationRequestsModel.REAPED_FLAG_KEY, reapedCondition);
        
        request.setQueryFilter(queryFilters);
        return request;
    }

    /**
     * Helper to create QueryRequest when querying the MitigationInstances table.
     * @param deviceName DeviceName for the workflow whose requests are being queried.
     * @param workflowIdStr WorkflowId represented as string, whose instances are being queried.
     * @param lastEvaluatedKey Represents the lastEvaluatedKey returned by DDB for the previous key, null if this is the first query.
     * @return QueryRequest representing the query to be issued against DDB to get the appropriate list of instances that need to be reaped.
     */
    protected QueryRequest createQueryForInstances(@Nonnull String deviceName, @Nonnull String workflowIdStr, @Nonnull Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest request = new QueryRequest(getInstancesTableName());
        String deviceWorkflowKey = MitigationInstancesModel.getDeviceWorkflowId(deviceName, workflowIdStr);
        
        Map<String, Condition> queryConditions = new HashMap<>();
        List<AttributeValue> conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue(deviceWorkflowKey));
        queryConditions.put(MitigationInstancesModel.DEVICE_WORKFLOW_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
        request.setKeyConditions(queryConditions);
        
        request.setConsistentRead(true);
        request.setAttributesToGet(Lists.newArrayList(MitigationInstancesModel.SCHEDULING_STATUS_KEY, MitigationInstancesModel.LOCATION_KEY, 
                                                      MitigationInstancesModel.MITIGATION_STATUS_KEY, MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        if (lastEvaluatedKey != null) {
            request.setExclusiveStartKey(lastEvaluatedKey);
        }
        return request;
    }
    
    /**
     * Responsible for querying the mitigation instances which aren't marked as complete, for the workflow represented by the input parameters. Protected for unit tests.
     * @param deviceName DeviceName for the workflow whose requests are being queried.
     * @param workflowIdStr WorkflowId represented as string, whose instances are being queried. 
     * @return Map whose key is the locations that needs to be reaped and whose value is an attribute-value Map for representing the state of the location.
     */
    protected Map<String, Map<String, AttributeValue>> queryInstancesForWorkflow(@Nonnull String deviceName, @Nonnull String workflowIdStr) {
        Map<String, Map<String, AttributeValue>> instancesDetails = new HashMap<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            QueryRequest request = createQueryForInstances(deviceName, workflowIdStr, lastEvaluatedKey);
            
            QueryResult result = queryDynamoDB(request);
            lastEvaluatedKey = result.getLastEvaluatedKey();
            
            if ((result == null) || (result.getCount() == 0)) {
                continue;
            }
            
            for (Map<String, AttributeValue> item : result.getItems()) {
                String location = item.get(MitigationInstancesModel.LOCATION_KEY).getS();
                instancesDetails.put(location, item);
            }
        } while (lastEvaluatedKey != null);
        
        return instancesDetails;
    }
    
    /**
     * Responsible for querying SWF API to identify if the workflow run represented by the input parameters is considered Closed by SWF. Protected to allow unit-tests.
     * @param deviceName Device corresponding to the workflow run whose SWF status is being queried.
     * @param workflowIdStr WorkflowId represented as String
     * @param metrics
     * @return boolean flag indicating true if the workflow represented by the input parameters is considered Closed by SWF, false otherwise. 
     */
    protected boolean isWorkflowClosedInSWF(@Nonnull String deviceName, @Nonnull String workflowIdStr, @Nonnull TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("isWorkflowClosedInSWF");
        try {
            DateTime now = new DateTime(DateTimeZone.UTC);
            String swfWorkflowId = deviceName + SWF_WORKFLOW_ID_KEY_SEPARATOR + workflowIdStr;
            
            ListClosedWorkflowExecutionsRequest listExecutionsRequest = new ListClosedWorkflowExecutionsRequest();
            listExecutionsRequest.setDomain(getSWFDomain());
            
            WorkflowExecutionFilter executionFilter = new WorkflowExecutionFilter();
            executionFilter.setWorkflowId(swfWorkflowId);
            listExecutionsRequest.setExecutionFilter(executionFilter);
            
            WorkflowExecutionInfos listClosedWorkflowResult = null;
            String nextPageToken = null;
            List<WorkflowExecutionInfo> workflowExecutionInfos = new ArrayList<>();
            int numAttempts = 0;
            do {
                while (numAttempts++ < MAX_SWF_QUERY_ATTEMPTS) {
                    try {
                        listClosedWorkflowResult = getSWFClient().listClosedWorkflowExecutions(listExecutionsRequest);
                        if ((listClosedWorkflowResult != null) && (listClosedWorkflowResult.getExecutionInfos() != null) &&
                            !listClosedWorkflowResult.getExecutionInfos().isEmpty()) {
                            workflowExecutionInfos.addAll(listClosedWorkflowResult.getExecutionInfos());
                        }
                        nextPageToken = listClosedWorkflowResult.getNextPageToken();
                        break;
                    } catch (Exception ex) {
                        String msg = "Caught exception when querying status in SWF for workflow: " + workflowIdStr + ", deviceName: " + deviceName;
                        LOG.warn(msg, ex);
                        
                        if (numAttempts < MAX_SWF_QUERY_ATTEMPTS) {
                            try {
                                Thread.sleep(SWF_RETRY_SLEEP_MILLIS_MULTIPLER * numAttempts);
                            } catch (InterruptedException ignored) {}
                        }
                    }
                }
            } while ((nextPageToken != null) && (numAttempts < MAX_SWF_QUERY_ATTEMPTS));
            
            if (workflowExecutionInfos.size() > 1) {
                String msg = "Got more than 1 WorkflowExecution when querying status in SWF for workflow: " + workflowIdStr + ", deviceName: " + deviceName + 
                             " WorkflowExecutionInfos found: " + workflowExecutionInfos;
                throw new RuntimeException(msg);
            }
            
            WorkflowExecutionInfo executionInfo = workflowExecutionInfos.get(0);
            
            String executionStatus = executionInfo.getExecutionStatus();
            if (executionStatus.equals(ExecutionStatus.CLOSED.name())) {
                // If SWF has marked this workflow as being finished, check how long ago did this happen, giving the
                // deciders some time to perform the appropriate DDB updates.
                DateTime closeTimestamp = new DateTime(executionInfo.getCloseTimestamp());
                if (now.minusSeconds(SECONDS_TO_ALLOW_WORKFLOW_DDB_UPDATES).isAfter(closeTimestamp)) {
                    return true;
                }
            }
            return false;
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
    
    protected String getRequestsTableName() {
        return mitigationRequestsTableName;
    }
    
    protected String getInstancesTableName() {
        return mitigationInstancesTableName;
    }
    
    protected String getSWFDomain() {
        return swfDomain;
    }
    
    protected AmazonSimpleWorkflowClient getSWFClient() {
        return swfClient;
    }
    
    protected int getMaxSecondsToStartWorkflow() {
        return maxSecondsToStartWorkflow;
    }

    protected SWFWorkflowStarter getWorkflowStarter() {
        return workflowStarter;
    }
}
