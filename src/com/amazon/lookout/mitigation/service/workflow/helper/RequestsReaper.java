package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
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
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.workflow.model.RequestToReap;
import com.amazon.lookout.workflow.reaper.RequestReaperConstants;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.SendHeartbeatOptions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.ExecutionStatus;
import com.amazonaws.services.simpleworkflow.model.ExecutionTimeFilter;
import com.amazonaws.services.simpleworkflow.model.ListClosedWorkflowExecutionsRequest;
import com.amazonaws.services.simpleworkflow.model.ListOpenWorkflowExecutionsRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionAlreadyStartedException;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionFilter;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfos;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

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
    
    // Number of seconds to attempt to reap request
    private static final int SECONDS_TO_ATTEMPT_TO_REAP_REQUEST_AFTER_CREATION = 60 * 60 * 24 * 7;//7 days.
    
    private static final int MAX_SWF_QUERY_ATTEMPTS = 3;
    private static final int SWF_RETRY_SLEEP_MILLIS_MULTIPLER = 100;
    
    private static final String SWF_WORKFLOW_ID_KEY_SEPARATOR = "_";
    
    // Keys to be used for publishing PMET metrics.
    private static final String NUM_REQUESTS_TO_REAP_METRIC_KEY = "NumRequestsToReap";
    private static final String FAILED_TO_START_WORKFLOW_METRIC_KEY = "FailedToStartWorkflow";
    private static final String NUM_REAPER_WORKFLOWS_STARTED_METRIC_KEY = "NumReaperWorkflows";
    private static final String PRE_EXISTING_REAPER_WORKFLOW_METRIC_KEY = "NumPreExistingReaperWorkflows";
    private static final String NUM_OPEN_WORKFLOW_EXECUTIONS_FOUND = "NumOpenWorkflowExecutionsFound";
    private static final String NUM_CLOSED_WORKFLOW_EXECUTIONS_FOUND = "NumClosedWorkflowExecutionsFound";
    private static final String REAPER_FINISHED_PASS_FORMAT = "ReaperFinishedPass.%s";
    
    private static final RecursiveToStringStyle recursiveToStringStyle = new RecursiveToStringStyle();
    
    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSimpleWorkflowClient swfClient;
    private final MetricsFactory metricsFactory;
    private final String mitigationRequestsTableName;
    private final String mitigationInstancesTableName;
    private final String swfDomain;
    private final int maxSecondsToStartWorkflow;
    private final SWFWorkflowStarter workflowStarter;
    private final int queryLimit;
    
    private final Map<DeviceName, Map<String, AttributeValue>> lastEvaluatedKeys = new EnumMap<>(DeviceName.class);
    private static final ImmutableSet<String> REQUESTS_QUERY_KEY_ATTRIBUTES =
            ImmutableSet.of(MitigationRequestsModel.DEVICE_NAME_KEY,  MitigationRequestsModel.WORKFLOW_ID_KEY);
    
    // Maximum WorkflowId for each DeviceName such that only requests satisfying the key condition
    // WorkflowId > WorkflowIdLowerBound could need to be reaped in the future. In other words, all
    // requests with WorkflowId <= WorkflowIdLowerBound have already SUCCEEDED or been reaped. This
    // is used in the KeyCondition of the MITIGATION_REQUESTS query finding requests to reap and
    // its purpose is to minimize the number of items that must be scanned by that query.
    private final Map<DeviceName, Long> workflowIdLowerBounds = new EnumMap<>(DeviceName.class);
    
    // Flag reset to false when starting a new query to find requests that could be reaped for
    // a DeviceName and then set to true when an unsuccessful (WorkflowStatus != SUCCEEDED),
    // unreaped (Reaped != 'true') is found. As long as this flag remained false for a given
    // DeviceName WorkflowIdLowerBound may continue to be increased as successful or reaped
    // requests are found but it must stop increasing once this flag is set to true.
    private final Map<DeviceName, Boolean> unsuccessfulUnreapedRequestWasFound = new EnumMap<>(DeviceName.class);
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Checkpoint {
        private Map<DeviceName, Map<String, AttributeValue>> lastEvaluatedKeys;
        private Map<DeviceName, Long> workflowIdLowerBounds;
        private Map<DeviceName, Boolean> unsuccessfulUnreapedRequestWasFound;
        
        private static ObjectMapper OM = new ObjectMapper().setSerializationInclusion(Include.NON_NULL);
        
        public static Checkpoint fromLockData(byte[] data) throws JsonParseException, JsonMappingException, IOException {
            return OM.readValue(data, Checkpoint.class);
        }
        
        public byte[] toLockData() throws JsonProcessingException {
            return OM.writeValueAsBytes(this);
        }
    }
    
    // lock client and related parameters for leadership election
    private static final String LEADER_ELECTION_LOCK_TABLE_NAME_FORMAT = "MITIGATION_SERVICE_LOCKS_%s";
    private static final long LEADER_ELECTION_LOCK_TABLE_READ_UNITS = 10L;
    private static final long LEADER_ELECTION_LOCK_TABLE_WRITE_UNITS = 10L;
    private static final String LEADER_ELECTION_LOCK_OWNER_NAME_FORMAT = "MitigationService-%s";
    private static final long LEADER_ELECTION_LOCK_HEARTBEAT_PERIOD_MILLIS = 1000L;
    private static final long MIN_LEADER_ELECTION_LEASE_DURATION_MILLIS = LEADER_ELECTION_LOCK_HEARTBEAT_PERIOD_MILLIS*3;
    private static final String LEADER_ELECTION_LOCK_KEY = "RequestsReaper";
    
    private final String leaderElectionLockTableName;
    private final AmazonDynamoDBLockClient leaderElectionLockClient;
    private LockItem leaderElectionLock;
    
    @ConstructorProperties({"dynamoDBClient", "swfClient", "appDomain", "swfDomainName",
            "swfSocketTimeoutMillis", "swfConnTimeoutMillis", "workflowStarter", "metricsFactory",
            "queryLimit", "leaderElectionLeaseDurationMillis"})
    public RequestsReaper(@NonNull AmazonDynamoDB dynamoDBClient, @NonNull AmazonSimpleWorkflowClient swfClient,
            @NonNull String appDomain, @NonNull String swfDomain, int swfSocketTimeoutMillis, int swfConnTimeoutMillis,
            @NonNull SWFWorkflowStarter workflowStarter, @NonNull MetricsFactory metricsFactory, int queryLimit,
            long leaderElectionLeaseDurationMillis) {
        this.dynamoDBClient = dynamoDBClient;
        this.swfClient = swfClient;
        
        Validate.notEmpty(appDomain);
        this.mitigationRequestsTableName = MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + appDomain.toUpperCase();
        this.mitigationInstancesTableName = MitigationInstancesModel.MITIGATION_INSTANCES_TABLE_NAME_PREFIX + appDomain.toUpperCase();
        
        Validate.notEmpty(swfDomain);
        this.swfDomain = swfDomain;
        
        Validate.isTrue(swfConnTimeoutMillis > 0);
        Validate.isTrue(swfSocketTimeoutMillis > 0);
        maxSecondsToStartWorkflow = swfConnTimeoutMillis/1000 + swfSocketTimeoutMillis/1000 + BUFFER_SECONDS_BEFORE_STARTING_WORKFLOW;
        
        this.workflowStarter = workflowStarter;
        this.metricsFactory = metricsFactory;
        
        Validate.isTrue(queryLimit >= 10, "queryLimit must be >= 10");
        this.queryLimit = queryLimit;
        
        Validate.isTrue(leaderElectionLeaseDurationMillis >= MIN_LEADER_ELECTION_LEASE_DURATION_MILLIS,
                "leaderElectionLeaseDurationMillis must be >= %d", MIN_LEADER_ELECTION_LEASE_DURATION_MILLIS);
        
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostName = String.format("randomhost-%06.0f", Math.random()*1e6);
        }
        
        leaderElectionLockTableName = String.format(LEADER_ELECTION_LOCK_TABLE_NAME_FORMAT, appDomain.toUpperCase());
        leaderElectionLockClient = new AmazonDynamoDBLockClient(new AmazonDynamoDBLockClientOptions()
            .withDynamoDB(dynamoDBClient)
            .withTableName(leaderElectionLockTableName)
            .withOwnerName(String.format(LEADER_ELECTION_LOCK_OWNER_NAME_FORMAT, hostName))
            .withLeaseDuration(leaderElectionLeaseDurationMillis)
            .withHeartbeatPeriod(LEADER_ELECTION_LOCK_HEARTBEAT_PERIOD_MILLIS)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withCreateHeartbeatBackgroundThread(false));
        leaderElectionLock = null;
    }

    @PostConstruct
    public void prepareToRun() {
        if (!leaderElectionLockClient.lockTableExists()) {
            LOG.info(String.format("Leader election Lock table %s does not exist in DynamoDB. Creating it...",
                    leaderElectionLockTableName));
            AmazonDynamoDBLockClient.createLockTableInDynamoDB(dynamoDBClient,
                    new ProvisionedThroughput()
                        .withReadCapacityUnits(LEADER_ELECTION_LOCK_TABLE_READ_UNITS)
                        .withWriteCapacityUnits(LEADER_ELECTION_LOCK_TABLE_WRITE_UNITS),
                    leaderElectionLockTableName);
            
            try { 
                TableUtils.waitUntilActive(dynamoDBClient, leaderElectionLockTableName);
            } catch (TableNeverTransitionedToStateException ex) {
                throw new RuntimeException(String.format("Failed creating table %s.", leaderElectionLockTableName), ex);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while waiting for table creation!", ex);
            }
            
            LOG.info(String.format("Table %s is ready for use.", leaderElectionLockTableName));
        }
    }
    
    @PreDestroy
    public void cleanup() {
        try {
            leaderElectionLockClient.close();
        } catch (Exception ex) {
            LOG.error("Failed to close leader election lock client!", ex);
        }
    }
    
    private Checkpoint getCheckpoint() {
        return new Checkpoint(lastEvaluatedKeys, workflowIdLowerBounds, unsuccessfulUnreapedRequestWasFound);
    }
    
    private void applyCheckpoint(Checkpoint checkpoint) {
        this.lastEvaluatedKeys.clear();
        if (checkpoint.getLastEvaluatedKeys() != null) {
            checkpoint.getLastEvaluatedKeys().forEach((deviceName, lastEvaluatedKey) -> {
                if (lastEvaluatedKey != null && lastEvaluatedKey.keySet().equals(REQUESTS_QUERY_KEY_ATTRIBUTES)) {
                    this.lastEvaluatedKeys.put(deviceName, lastEvaluatedKey);
                } else {
                    LOG.warn("Ignoring lastEvaluatedKey:" + lastEvaluatedKey
                            + " with unexpected attributes for deviceName:" + deviceName);
                }
            });
        }
        if (checkpoint.getWorkflowIdLowerBounds() != null) {
            this.workflowIdLowerBounds.putAll(checkpoint.getWorkflowIdLowerBounds());
        }
        this.unsuccessfulUnreapedRequestWasFound.clear();
        if (checkpoint.getUnsuccessfulUnreapedRequestWasFound() != null) {
            this.unsuccessfulUnreapedRequestWasFound.putAll(checkpoint.getUnsuccessfulUnreapedRequestWasFound());
        }
    }
    
    @Override
    public void run() {
        try (TSDMetrics metrics = new TSDMetrics(getMetricsFactory(), "RunRequestsReaper")) {
            try {
                // attempt to acquire and/or maintain the leader election lock
                if (leaderElectionLock == null || leaderElectionLock.isExpired()) {
                    // attempt to acquire leader election lock
                    leaderElectionLock = leaderElectionLockClient.acquireLock(new AcquireLockOptions()
                        .withKey(LEADER_ELECTION_LOCK_KEY)
                        .withRefreshPeriod(LEADER_ELECTION_LOCK_HEARTBEAT_PERIOD_MILLIS)
                        .withAdditionalTimeToWaitForLock(LEADER_ELECTION_LOCK_HEARTBEAT_PERIOD_MILLIS)
                        .withTimeUnit(TimeUnit.MILLISECONDS)
                        .withDeleteLockOnRelease(false)
                        .withReplaceData(false));
                    LOG.info(String.format("Successfully acquired %s lock in table %s.",
                            LEADER_ELECTION_LOCK_KEY, leaderElectionLockTableName));
                    
                    // de-serialize and apply checkpoint data from lock
                    byte[] lockData = leaderElectionLock.getData();
                    if (lockData != null) {
                        try {
                            applyCheckpoint(Checkpoint.fromLockData(lockData));
                            LOG.info("Successfully applied checkpoint data from lock: "
                                    + new String(lockData, StandardCharsets.UTF_8));
                        } catch (Exception ex) {
                            LOG.warn("Failed to apply checkpoint data from lock: "
                                    + new String(lockData, StandardCharsets.UTF_8), ex);
                        }
                    } else {
                        LOG.info("Did not find any checkpoint data attached to lock.");
                    }
                } else {
                    // send heartbeat to ensure we own the lock
                    leaderElectionLockClient.sendHeartbeat(new SendHeartbeatOptions()
                        .withLockItem(leaderElectionLock));
                    LOG.info(String.format("Successfully sent heartbeat for %s lock in table %s.",
                            LEADER_ELECTION_LOCK_KEY, leaderElectionLockTableName));
                }
                
                // we are the leader and should reap requests, because the preceding code throws
                // LockNotGrantedException when we don't own the leader election lock.
                reapRequests(metrics);
                
                // send another heartbeat to keep holding the lock
                leaderElectionLockClient.sendHeartbeat(new SendHeartbeatOptions()
                    .withLockItem(leaderElectionLock)
                    .withData(getCheckpoint().toLockData()));
                LOG.info(String.format("Successfully sent heartbeat for %s lock in table %s.",
                        LEADER_ELECTION_LOCK_KEY, leaderElectionLockTableName));
            } catch (LockNotGrantedException ex) {
                LOG.info(String.format("Not running reaper because %s lock in table %s was not acquired: %s",
                        LEADER_ELECTION_LOCK_KEY, leaderElectionLockTableName, ex.getMessage()));
                leaderElectionLock = null;
            } catch (InterruptedException ex) {
                // Don't log InterruptedException as an ERROR, because it can be logged during shutdown.
                LOG.info("Interrupted! Probably shutting down...");
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                // Simply catch all exceptions and log them as errors, to prevent this thread from dying.
                LOG.error(ex);
            } finally {
                metrics.addCount("LeaderElectionLocksOwned", leaderElectionLock != null ? 1 : 0);
            }
        }
    }
    
    // protected to be visible for mocking
    protected Map<String, AttributeValue> getLastEvaluatedKey(DeviceName deviceName) {
        return lastEvaluatedKeys.get(deviceName);
    }
    
    // protected to be visible for mocking
    protected void setLastEvaluatedKey(DeviceName deviceName, Map<String, AttributeValue> lastEvaluatedKey) {
        lastEvaluatedKeys.put(deviceName, lastEvaluatedKey);
    }
    
    // protected to be visible for mocking
    protected long getWorkflowIdLowerBound(DeviceName deviceName) {
        return workflowIdLowerBounds.getOrDefault(deviceName, 0L);
    }
    
    // protected to be visible for mocking
    protected void setWorkflowIdLowerBound(DeviceName deviceName, long workflowId) {
        workflowIdLowerBounds.put(deviceName, workflowId);
    }
    
    // protected to be visible for mocking
    protected boolean wasUnsuccessfulUnreapedRequestFound(DeviceName deviceName) {
        return unsuccessfulUnreapedRequestWasFound.getOrDefault(deviceName, Boolean.FALSE);
    }
    
    // protected to be visible for mocking
    protected void setUnsuccessfulUnreapedRequestWasFound(DeviceName deviceName, boolean wasFound) {
        unsuccessfulUnreapedRequestWasFound.put(deviceName, wasFound);
    }
    
    /**
     * Entry point for this workflow reaper. Protected for unit-testing.
     * It first queries all Requests to identify ones that need to be reaped, along with the locations that correspond to that request which needs updating.
     * Each of one such requests to be reaped is wrapped in a RequestToReap object.
     * It next starts a workflow per RequestToReap instance, setting the workflowId as the RequestDeviceName + <SEPARATOR> OriginalWorkflowId + <SEPARATOR> + Reaper.
     * This ensures only one of such workflow would be running at any point in time.
     */
    protected void reapRequests(@NonNull TSDMetrics tsdMetrics) {
        TSDMetrics metrics = tsdMetrics.newSubMetrics("reapRequests");
        try {
            metrics.addZero(NUM_REQUESTS_TO_REAP_METRIC_KEY);
            metrics.addZero(FAILED_TO_START_WORKFLOW_METRIC_KEY);
            metrics.addZero(NUM_REAPER_WORKFLOWS_STARTED_METRIC_KEY);
            metrics.addZero(PRE_EXISTING_REAPER_WORKFLOW_METRIC_KEY);
            metrics.addZero(NUM_OPEN_WORKFLOW_EXECUTIONS_FOUND);
            metrics.addZero(NUM_CLOSED_WORKFLOW_EXECUTIONS_FOUND);
            
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
    protected List<RequestToReap> getRequestsToReap(@NonNull TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("getWorkflowsToReap");
        int numRequestsFound = 0;
        int numRequestsScanned = 0;
        int numRequestsIgnored = 0;
        int numExpiredRequestsFound = 0;
        int numRequestsFoundRunning = 0;
        int numDevicesFailingReaperCheck = 0;
        try {
            List<RequestToReap> requestsToBeReaped = new ArrayList<>();
            
            // Query DDB for active workflows. We have a LSI on DeviceName + UpdateWorkflowId, hence query for each device.
            for (DeviceName deviceName : DeviceName.values()) {
                try {
                    // Get a list of active requests which weren't successful and haven't been reaped as yet.
                    Map<String, AttributeValue> lastEvaluatedKey = getLastEvaluatedKey(deviceName);
                    long workflowIdLowerBound = getWorkflowIdLowerBound(deviceName);
                    boolean unsuccessfulUnreapedRequestFound;
                    if (lastEvaluatedKey == null) {
                        // starting new query pass; reset flag
                        unsuccessfulUnreapedRequestFound = false;
                    } else {
                        // continuing a query pass; use saved flag
                        unsuccessfulUnreapedRequestFound = wasUnsuccessfulUnreapedRequestFound(deviceName);
                    }
                    
                    QueryResult queryResult = queryForRequests(deviceName.name(), lastEvaluatedKey, workflowIdLowerBound);

                    if (queryResult == null) {
                        // Happens in unit testing when the class is mocked
                        LOG.warn("queryResult was null for deviceName: " + deviceName.name());
                        continue;
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Found: " + queryResult.getItems().size() + " unsuccessful+unreaped requests: " + ReflectionToStringBuilder.toString(queryResult));
                    }
                    
                    numRequestsScanned += queryResult.getScannedCount();
                    
                    for (Map<String, AttributeValue> item : queryResult.getItems()) {
                        String workflowIdStr = item.get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN();
                        long workflowId;
                        try {
                            workflowId = Long.parseLong(workflowIdStr);
                        } catch (IllegalArgumentException ex) {
                            LOG.warn("Failed to parse WorkflowId string:" + workflowIdStr, ex);
                            workflowId = -1; // guaranteed to be < workflowIdLowerBound
                        }
                        
                        List<String> locations = item.get(MitigationRequestsModel.LOCATIONS_KEY).getSS();
                        String workflowStatus = item.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY).getS();
                        long requestDateInMillis = Long.parseLong(item.get(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY).getN());
                        DateTime requestDateTime = new DateTime(requestDateInMillis);
                        String mitigationName = item.get(MitigationRequestsModel.MITIGATION_NAME_KEY).getS();
                        int mitigationVersion = Integer.parseInt(item.get(MitigationRequestsModel.MITIGATION_VERSION_KEY).getN());
                        String mitigationTemplate = item.get(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY).getS();
                        String deviceScope = item.get(MitigationRequestsModel.DEVICE_SCOPE_KEY).getS();
                        String requestType = item.get(MitigationRequestsModel.REQUEST_TYPE_KEY).getS();
                        String serviceName = item.get(MitigationRequestsModel.SERVICE_NAME_KEY).getS();
                        AttributeValue reapedFlagValue = item.get(MitigationRequestsModel.REAPED_FLAG_KEY);
                        boolean reaped = reapedFlagValue != null && "true".equals(reapedFlagValue.getS());
                        
                        // SWF RunId could be null - for cases where a new request was persisted into DDB, but the SWF RunId was not yet updated.
                        AttributeValue swfRunIdValue = item.get(MitigationRequestsModel.SWF_RUN_ID_KEY);
                        String swfRunId = swfRunIdValue != null ? swfRunIdValue.getS() : null;
                        
                        // skip requests with WorkflowStatus == SUCCEEDED or Reaped == 'true'
                        if (workflowStatus.equals(WorkflowStatus.SUCCEEDED) || reaped) {
                            ++numRequestsIgnored;
                            if (!unsuccessfulUnreapedRequestFound) {
                                // Attempt to increase WorkflowIdLowerBound, because we have not found
                                // a previous unsuccessful unreaped request with a lower WorkflowId.
                                workflowIdLowerBound = Math.max(workflowIdLowerBound, workflowId);
                                LOG.debug("WorkflowIdLowerBound increased to " + workflowIdLowerBound);
                            }
                            continue;
                        }
                        
                        // If the request has a creation time before SECONDS_TO_ATTEMPT_TO_REAP_REQUEST_AFTER_CREATION, we stop attempting to reap it.
                        DateTime now = new DateTime(DateTimeZone.UTC);
                        if (now.minusSeconds(SECONDS_TO_ATTEMPT_TO_REAP_REQUEST_AFTER_CREATION).isAfter(requestDateInMillis)) {
                            LOG.error("Workflow: " + workflowIdStr + " for mitigation: " + mitigationName + " using template: " + mitigationTemplate +
                                      " at locations: " + locations + " has not being successfully reaped  within the last: " + SECONDS_TO_ATTEMPT_TO_REAP_REQUEST_AFTER_CREATION + 
                                      " number of seconds, stop attempting to reap. Workflow RequestDate: " + requestDateTime);
                            ++numExpiredRequestsFound;
                            if (!unsuccessfulUnreapedRequestFound) {
                                // Attempt to increase WorkflowIdLowerBound, because we have given
                                // up attempting to reap this request and have not found a previous
                                // unsuccessful unreaped request with a lower WorkflowId that was
                                // also not too old to be reaped.
                                workflowIdLowerBound = Math.max(workflowIdLowerBound, workflowId);
                                LOG.debug("WorkflowIdLowerBound increased to " + workflowIdLowerBound);
                            }
                            continue;
                        }
                        
                        // we found an unsuccessful and unreaped request
                        unsuccessfulUnreapedRequestFound = true;
                        ++numRequestsFound;
                        
                        // If the workflow status is Running, check if it has been running for at least N seconds, if not, then skip this entry.
                        if (workflowStatus.equals(WorkflowStatus.RUNNING)) {
                            ++numRequestsFoundRunning;
                            
                            if (now.minusSeconds(getMaxSecondsToStartWorkflow()).isBefore(requestDateInMillis)) {
                                LOG.debug("Workflow: " + workflowIdStr + " for mitigation: " + mitigationName + " using template: " + mitigationTemplate +
                                          " at locations: " + locations + " has recently started within the last: " + getMaxSecondsToStartWorkflow() + 
                                          " number of seconds, hence not reaping. Workflow RequestDate: " + requestDateTime);
                                continue;
                            }
                            
                            // Query SWF to see if this workflow is still active or not. If it is still active, don't perform any further steps.
                            if (!isWorkflowClosedInSWF(deviceName.name(), workflowIdStr, swfRunId, requestDateInMillis, subMetrics)) {
                                LOG.debug("Workflow: " + workflowIdStr + " for mitigation: " + mitigationName + " using template: " + mitigationTemplate +
                                            " at locations: " + locations + " with RequestDate: " + requestDateTime + " doesn't show as CLOSED in SWF, hence skipping any reaping activity.");
                                continue;
                            }
                        }
                        
                        // Get instances for the workflow corresponding to the request being evaluated.
                        Map<String, Map<String, AttributeValue>> instancesDetails = queryInstancesForWorkflow(deviceName.name(), workflowIdStr);
                        
                        // Filter only the instances that need some state to be corrected.
                        Map<String, Map<String, AttributeValue>> instancesToBeReaped = filterInstancesToBeReaped(locations, instancesDetails);
                        LOG.debug("Workflow: " + workflowIdStr + " for mitigation: " + mitigationName + " using template: " + mitigationTemplate +
                                    " at locations: " + locations + " has " +  instancesToBeReaped.size() + " instances to be reaped. Instances: " + 
                                  ReflectionToStringBuilder.toString(instancesToBeReaped));
                        
                        // Wrap up this request and its instances in a RequestToReap object. 
                        RequestToReap workflowInfo = new RequestToReap(workflowIdStr, swfRunId, deviceName.name(), deviceScope, serviceName, mitigationName, 
                                                                       mitigationVersion, mitigationTemplate, requestType, requestDateInMillis, instancesToBeReaped);
                        LOG.info("Request that needs to be reaped: " + workflowInfo);
                        requestsToBeReaped.add(workflowInfo);
                    }
                    
                    setWorkflowIdLowerBound(deviceName, workflowIdLowerBound);
                    setUnsuccessfulUnreapedRequestWasFound(deviceName, unsuccessfulUnreapedRequestFound);
                    
                    lastEvaluatedKey = queryResult.getLastEvaluatedKey();
                    setLastEvaluatedKey(deviceName, lastEvaluatedKey);
                    subMetrics.addCount(String.format(REAPER_FINISHED_PASS_FORMAT, deviceName), lastEvaluatedKey == null ? 1 : 0);

                } catch (Exception ex) {
                    // Handle any exceptions by logging a warning and moving on to the next device.
                    String msg = "Caught exception when querying active workflows for device: " + deviceName.name();
                    LOG.warn(msg, ex);
                    ++numDevicesFailingReaperCheck;
                    break; // break out of the do-while loop.
                }
            }
            return requestsToBeReaped;
        } finally {
            subMetrics.addCount("NumRequestsFound", numRequestsFound);
            subMetrics.addCount("NumRequestsScanned", numRequestsScanned);
            subMetrics.addCount("NumRequestsIgnored", numRequestsIgnored);
            subMetrics.addCount("NumExpiredRequestsFound", numExpiredRequestsFound);
            subMetrics.addCount("NumRequestsFoundRunning", numRequestsFoundRunning);
            subMetrics.addCount("NumDevicesFailingReaperCheck", numDevicesFailingReaperCheck);
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
     * Query the MITIGATION_REQUESTS table in DynamoDB to find requests that might need to be reaped.
     * 
     * @param deviceName DeviceName of requests to query for.
     * @param lastEvaluatedKey last key evaluated by previous DynamoDB query for the same DeviceName.
     * @param workflowIdLowerBound Lower bound for WorkflowId key condition.
     * @return DynamoDB QueryResult containing requests that might need to be reaped.
     */
    protected QueryResult queryForRequests(@NonNull String deviceName,
            Map<String, AttributeValue> lastEvaluatedKey, long workflowIdLowerBound) {
        QueryRequest request = createQueryForRequests(deviceName, lastEvaluatedKey, workflowIdLowerBound);
        return queryDynamoDB(request);
    }
    
    /**
     * Create DynamoDB QueryRequest to find requests that might need to be reaped.
     * 
     * @param deviceName DeviceName of requests to query for.
     * @param lastEvaluatedKey last key evaluated by previous DynamoDB query for the same DeviceName.
     * @param workflowIdLowerBound Lower bound for WorkflowId key condition.
     * @return DynamoDB QueryRequest used to find request that might need to be reaped.
     */
    protected QueryRequest createQueryForRequests(@NonNull String deviceName,
            Map<String, AttributeValue> lastEvaluatedKey, long workflowIdLowerBound) {
        QueryRequest request = new QueryRequest(getRequestsTableName());
        Map<String, Condition> queryConditions = new HashMap<>();
        
        queryConditions.put(MitigationRequestsModel.DEVICE_NAME_KEY,
                new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(
                        new AttributeValue().withS(deviceName)));
        
        if (lastEvaluatedKey != null) {
            // if we have a lastEvaluatedKey then continue query from where we left off
            request.setExclusiveStartKey(lastEvaluatedKey);
        } else {
            // otherwise start a new query from workflowIdLowerBound
            queryConditions.put(MitigationRequestsModel.WORKFLOW_ID_KEY,
                    new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(
                            new AttributeValue().withN(Long.toString(workflowIdLowerBound))));
        }
        
        request.setKeyConditions(queryConditions);
        request.setConsistentRead(true);
        request.setLimit(queryLimit);
        request.setAttributesToGet(Lists.newArrayList(MitigationRequestsModel.WORKFLOW_ID_KEY,
                                                      MitigationRequestsModel.SWF_RUN_ID_KEY, 
                                                      MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY,
                                                      MitigationRequestsModel.WORKFLOW_STATUS_KEY, 
                                                      MitigationRequestsModel.DEVICE_SCOPE_KEY,
                                                      MitigationRequestsModel.LOCATIONS_KEY, 
                                                      MitigationRequestsModel.MITIGATION_NAME_KEY,
                                                      MitigationRequestsModel.MITIGATION_VERSION_KEY, 
                                                      MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY,
                                                      MitigationRequestsModel.REQUEST_TYPE_KEY,
                                                      MitigationRequestsModel.SERVICE_NAME_KEY,
                                                      MitigationRequestsModel.REAPED_FLAG_KEY));
        
        LOG.debug("From deviceName:" + deviceName + ", lastEvaluatedKey:" + lastEvaluatedKey
                + ", workflowIdLowerBound:" + workflowIdLowerBound + " created query request:"
                + request);

        return request;
    }

    /**
     * Helper to create QueryRequest when querying the MitigationInstances table.
     * @param deviceName DeviceName for the workflow whose requests are being queried.
     * @param workflowIdStr WorkflowId represented as string, whose instances are being queried.
     * @param lastEvaluatedKey Represents the lastEvaluatedKey returned by DDB for the previous key, null if this is the first query.
     * @return QueryRequest representing the query to be issued against DDB to get the appropriate list of instances that need to be reaped.
     */
    protected QueryRequest createQueryForInstances(@NonNull String deviceName, @NonNull String workflowIdStr, Map<String, AttributeValue> lastEvaluatedKey) {
        QueryRequest request = new QueryRequest(getInstancesTableName());
        String deviceWorkflowKey = MitigationInstancesModel.getDeviceWorkflowId(deviceName, workflowIdStr);
        
        Map<String, Condition> queryConditions = new HashMap<>();
        List<AttributeValue> conditionAttributes = new ArrayList<>();
        conditionAttributes.add(new AttributeValue(deviceWorkflowKey));
        queryConditions.put(MitigationInstancesModel.DEVICE_WORKFLOW_ID_KEY, new Condition().withAttributeValueList(conditionAttributes).withComparisonOperator(ComparisonOperator.EQ));
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
    protected Map<String, Map<String, AttributeValue>> queryInstancesForWorkflow(@NonNull String deviceName, @NonNull String workflowIdStr) {
        Map<String, Map<String, AttributeValue>> instancesDetails = new HashMap<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            QueryRequest request = createQueryForInstances(deviceName, workflowIdStr, lastEvaluatedKey);
            
            QueryResult result = queryDynamoDB(request);
            lastEvaluatedKey = result.getLastEvaluatedKey();

            if (result.getCount() == 0) {
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
     * @param swfRunId SWF run ID, represented as String, used for identify workflow run with same workflow id.
     * @param requestDateInMillis Timestamp in millis of when this request was created.
     * @param metrics
     * @return boolean flag indicating true if the workflow represented by the input parameters is considered Closed by SWF, false otherwise. 
     */
    protected boolean isWorkflowClosedInSWF(@NonNull String deviceName, @NonNull String workflowIdStr, String swfRunId, long requestDateInMillis, @NonNull TSDMetrics metrics) {
        try (TSDMetrics subMetrics = metrics.newSubMetrics("isWorkflowClosedInSWF")) {
            DateTime now = new DateTime(DateTimeZone.UTC);
            String swfWorkflowId = deviceName + SWF_WORKFLOW_ID_KEY_SEPARATOR + workflowIdStr;

            List<WorkflowExecutionInfo> openExecutions = getOpenExecutions(swfWorkflowId, swfRunId, requestDateInMillis);
            List<WorkflowExecutionInfo> closedExecutions = getClosedExecutions(swfWorkflowId, swfRunId, requestDateInMillis);
            
            if (closedExecutions.isEmpty()) {
                // If no closed workflows found, then return false if the workflow is still open
                // and return true if it doesn't even exist
                return openExecutions.isEmpty();
            }
            
            if (closedExecutions.size() + openExecutions.size() > 1) {
                LOG.warn( String.format("Found multiple executions of workflow: %s, deviceName: %s, closed executions: %s, open executions: %s",
                        workflowIdStr, deviceName, closedExecutions, openExecutions) );
            }
            
            // find the execution that started most recently (either open or closed)
            WorkflowExecutionInfo latestExecution = null;
            boolean latestExecutionIsOpen = false;
            for (WorkflowExecutionInfo execution : openExecutions) {
                if (latestExecution == null || execution.getStartTimestamp().after(latestExecution.getStartTimestamp())) {
                    latestExecution = execution;
                    latestExecutionIsOpen = true;
                }
            }
            for (WorkflowExecutionInfo execution : closedExecutions) {
                if (latestExecution == null || execution.getStartTimestamp().after(latestExecution.getStartTimestamp())) {
                    latestExecution = execution;
                    latestExecutionIsOpen = false;
                }
            }
            LOG.debug( String.format("Found latest execution %s (which is %s) for workflow: %s, deviceName: %s",
                    latestExecution, latestExecutionIsOpen ? "open" : "closed", workflowIdStr, deviceName) );
                        
            subMetrics.addCount(NUM_OPEN_WORKFLOW_EXECUTIONS_FOUND, latestExecutionIsOpen ? 1 : 0);
            subMetrics.addCount(NUM_CLOSED_WORKFLOW_EXECUTIONS_FOUND, latestExecutionIsOpen ? 0 : 1);
            
            if (!latestExecutionIsOpen) {
                String executionStatus = latestExecution.getExecutionStatus();
                if (executionStatus.equals(ExecutionStatus.CLOSED.name())) {
                    // If SWF has marked this workflow as being finished, check how long ago did this happen, giving the
                    // deciders some time to perform the appropriate DDB updates.
                    DateTime closeTimestamp = new DateTime(latestExecution.getCloseTimestamp());
                    if (now.minusSeconds(SECONDS_TO_ALLOW_WORKFLOW_DDB_UPDATES).isAfter(closeTimestamp)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private List<WorkflowExecutionInfo> getClosedExecutions(@NonNull String swfWorkflowId, String swfRunId, long requestDateInMillis) {
        ListClosedWorkflowExecutionsRequest request = buildListClosedWorkflowExecutionsRequest(
                swfWorkflowId,
                requestDateInMillis);
        List<WorkflowExecutionInfo> closedExecutions = getWorkflowExecutions(
                request,
                (previousRequest, nextPageToken) -> getSWFClient().listClosedWorkflowExecutions(
                        previousRequest.withNextPageToken(nextPageToken)));

        return filterWithSWFRunId(closedExecutions, swfRunId, request);
    }
    
    private List<WorkflowExecutionInfo> filterWithSWFRunId(List<WorkflowExecutionInfo> executions, String swfRunId,
            AmazonWebServiceRequest request) {
        // if swf run id is not null, filter the result with it.
        if (swfRunId != null) {
            executions = executions.stream()
                    .filter(workflow -> swfRunId.equals(workflow.getExecution().getRunId()))
                    .collect(Collectors.toList());
        }
        
        LOG.debug("Got workflows : " +
                ReflectionToStringBuilder.toString(executions, recursiveToStringStyle) +
                ", when querying SWF for closed workflows using the list Workflow Executions API with request: " +
                ReflectionToStringBuilder.toString(request, recursiveToStringStyle));
        
        return executions;
    }

    private List<WorkflowExecutionInfo> getOpenExecutions(String swfWorkflowId, String swfRunId, long requestDateInMillis) {
        ListOpenWorkflowExecutionsRequest request = buildListOpenWorkflowExecutionsRequest(
                swfWorkflowId,
                requestDateInMillis);
        List<WorkflowExecutionInfo> openExecutions = getWorkflowExecutions(
                request,
                (previousRequest, nextPageToken) -> getSWFClient().listOpenWorkflowExecutions(
                        previousRequest.withNextPageToken(nextPageToken)));
        return filterWithSWFRunId(openExecutions, swfRunId, request);
    }

    private ListClosedWorkflowExecutionsRequest buildListClosedWorkflowExecutionsRequest(String swfWorkflowId,
                long requestDateInMillis) {
        ListClosedWorkflowExecutionsRequest request = new ListClosedWorkflowExecutionsRequest();
        request.setDomain(getSWFDomain());
        request.setExecutionFilter(workflowIdExecutionFilter(swfWorkflowId));
        request.setStartTimeFilter(workflowStartTimeFilter(requestDateInMillis));
        return request;
    }

    private ListOpenWorkflowExecutionsRequest buildListOpenWorkflowExecutionsRequest(String swfWorkflowId,
                long requestDateInMillis) {
        ListOpenWorkflowExecutionsRequest request = new ListOpenWorkflowExecutionsRequest();
        request.setDomain(getSWFDomain());
        request.setExecutionFilter(workflowIdExecutionFilter(swfWorkflowId));
        request.setStartTimeFilter(workflowStartTimeFilter(requestDateInMillis));
        return request;
    }

    private WorkflowExecutionFilter workflowIdExecutionFilter(String swfWorkflowId) {
        WorkflowExecutionFilter executionFilter = new WorkflowExecutionFilter();
        executionFilter.setWorkflowId(swfWorkflowId);
        return executionFilter;
    }

    private ExecutionTimeFilter workflowStartTimeFilter(long requestDateInMillis) {
        // We subtract a minute for the oldestStartTime constraint to ensure the SWF start time is definitely after the oldestStartTime.
        DateTime oldestStartTime = new DateTime(requestDateInMillis).minusMinutes(1);
        return new ExecutionTimeFilter().withOldestDate(oldestStartTime.toDate());
    }

    private <TRequest> List<WorkflowExecutionInfo> getWorkflowExecutions(
            TRequest request,
            BiFunction<TRequest, String, WorkflowExecutionInfos> call) {
        return executePaginatedCallWithRetries(
                request,
                call,
                resultPage -> resultPage.getNextPageToken(),
                resultPage -> resultPage.getExecutionInfos());
    }

    private <TRequest, TResultPage, TNextPageToken, TResultItem> List<TResultItem> executePaginatedCallWithRetries(
            TRequest request,
            BiFunction<TRequest, TNextPageToken, TResultPage> call,
            Function<TResultPage, TNextPageToken> getNextPageToken,
            Function<TResultPage, List<TResultItem>> getPageItems) {
        List<TResultItem> result = new ArrayList<>();
        TNextPageToken nextPageToken = null;
        int numAttempts = 0;
        do {
            while (numAttempts++ < MAX_SWF_QUERY_ATTEMPTS) {
                try {
                    TResultPage page = call.apply(request, nextPageToken);
                    if (page != null) {
                        List<TResultItem> pageItems = getPageItems.apply(page);
                        if (pageItems != null) {
                            result.addAll(pageItems);
                        }
                    }

                    nextPageToken = getNextPageToken.apply(page);
                    break; // break from retry loop
                } catch (Exception ex) {
                    String message = "Caught exception when querying status in SWF for workflow. Request: " +
                            ReflectionToStringBuilder.toString(request);
                    LOG.warn(message, ex);

                    if (numAttempts < MAX_SWF_QUERY_ATTEMPTS) {
                        Uninterruptibles.sleepUninterruptibly(
                                SWF_RETRY_SLEEP_MILLIS_MULTIPLER * numAttempts,
                                TimeUnit.MILLISECONDS);
                    } else {
                        throw ex;
                    }
                }
            }
        } while ((nextPageToken != null) && (numAttempts < MAX_SWF_QUERY_ATTEMPTS));

        return result;
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
