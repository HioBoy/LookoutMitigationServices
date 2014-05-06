package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.DeleteMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Sets;
import com.mallardsoft.tuple.Pair;

/**
 * DDBBasedDeleteRequestStorageHandler is responsible for persisting delete requests into DDB.
 * As part of persisting this request, based on the existing requests in the table, this handler also determines the workflowId to be used for the current request.
 */
public class DDBBasedDeleteRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedDeleteRequestStorageHandler.class);
    
    // Num Attempts to get new workflowId + retry sleep configs.
    public static final int NEW_WORKFLOW_ID_MAX_ATTEMPTS = 10;
    private static final int NEW_WORKFLOW_ID_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Prefix tags for logging warnings to be monitored.
    public static final String DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[DELETE_REQUEST_STORAGE_FAILED] ";
    
    // Keys for TSDMetric property.
    private static final String NUM_ACTIVE_MITIGATIONS_FOR_DEVICE = "NumActiveMitigations";
    private static final String NUM_ATTEMPTS_TO_STORE_DELETE_REQUEST = "NumDeleteRequestStoreAttempts";
    
    public DDBBasedDeleteRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain) {
        super(dynamoDBClient, domain);
    }

    /**
     * Stores the delete request into the DDB Table. While storing, it identifies the new workflowId to be associated with this request and returns back the same.
     * The algorithm it uses to identify the workflowId to use is:
     * 1. Identify the deviceNameAndScope that corresponds to this request.
     * 2. Query for all active mitigations for this device.
     * 3. If no active mitigations exist for this device throw back an exception since there is nothing to delete.
     * 4. Else identify the max workflowId for the active mitigations for this device and scope and try to use this maxWorkflowId+1 as the new workflowId
     *    when storing the request.
     * 5. If when storing the request we encounter an exception, it could be either because someone else started using the maxWorkflowId+1 workflowId for that device
     *    or it is some transient exception. In either case, we query the DDB table once again for mitigations >= maxWorkflowId for the device and
     *    continue with step 4. 
     * @param deleteRequest Request to be stored.
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@Nonnull MitigationModificationRequest request, @Nonnull TSDMetrics metrics) {
        Validate.notNull(request);
        Validate.notNull(metrics);

        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedDeleteRequestStorageHandler.storeRequestForWorkflow");
        int numAttempts = 0;
        try {
            DeleteMitigationRequest deleteRequest = (DeleteMitigationRequest) request;
            
            String mitigationName = deleteRequest.getMitigationName();
            String mitigationTemplate = deleteRequest.getMitigationTemplate();
            int mitigationVersion = deleteRequest.getMitigationVersion();

            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);

            String deviceName = deviceNameAndScope.getDeviceName().name();
            String deviceScope = deviceNameAndScope.getDeviceScope().name();

            Long maxWorkflowId = null;
            boolean activeMitigationToDeleteFound = false;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts++ < NEW_WORKFLOW_ID_MAX_ATTEMPTS) {
                // On evaluating active mitigations, we should get back the maxWorkflowId seen so far, along with a boolean flag indicating if the mitigation to delete exists.
                // The Pair<Long, Boolean> activeMitigationsEvalResult below holds the maxWorkflowId and the flag defined above.
                Pair<Long, Boolean> activeMitigationsEvalResult = evaluateActiveMitigations(deviceName, deviceScope, mitigationName, mitigationTemplate, 
                                                                                            mitigationVersion, maxWorkflowId, activeMitigationToDeleteFound, subMetrics);
                maxWorkflowId = Pair.get1(activeMitigationsEvalResult);
                activeMitigationToDeleteFound = Pair.get2(activeMitigationsEvalResult);

                long newWorkflowId = 0;
                // If we didn't get any active workflows for the same deviceName and deviceScope or if we didn't find any mitigation corresponding to our delete request, throw back an exception.
                if ((maxWorkflowId == null) || !activeMitigationToDeleteFound) {
                    String msg = "No active mitigation to delete found when querying for deviceName: " + deviceName + " deviceScope: " + deviceScope + 
                                 ", for request: " + ReflectionToStringBuilder.toString(deleteRequest);
                    LOG.warn(msg);
                    throw new RuntimeException(msg);
                } else {
                    // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                    newWorkflowId = ++maxWorkflowId;
                    sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);
                }

                try {
                    storeRequestInDDB(deleteRequest, deviceNameAndScope, newWorkflowId, RequestType.DeleteRequest.name(), deleteRequest.getMitigationVersion(), subMetrics);
                    return newWorkflowId;
                } catch (Exception ex) {
                    String msg = "Caught exception when storing delete request in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(deleteRequest);
                    LOG.warn(msg);

                    if (numAttempts < NEW_WORKFLOW_ID_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(getSleepMillisMultiplierBetweenStoreRetries() * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX + "- Unable to store delete request : " + ReflectionToStringBuilder.toString(deleteRequest) +
                         " after " + numAttempts + " attempts";
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_ATTEMPTS_TO_STORE_DELETE_REQUEST, numAttempts);
            subMetrics.end();
        }
    }
    
    /**
     * Evaluate the active mitigations currently in place for this device and scope - to extract the maxWorkflowId currently in use and also to check if the
     * mitigation that needs to be deleted is currently active.
     * Protected for unit-testing.
     * @param deviceName DeviceName on which the mitigation needs to be deleted.
     * @param deviceScope DeviceScope for the device where the mitigation needs to be deleted.
     * @param mitigationName Name of the mitigation being deleted.
     * @param mitigationTemplate Template that was used for creating the mitigation being deleted.
     * @param mitigationVersion Version of the mitigation that the client requests to be deleted. This helps us ensure the client has latest view of the mitigation to be deleted.
     * @param maxWorkflowIdOnLastAttempt If we had queried this DDB before, we could query for mitigations greaterThanOrEqual to this maxWorkflowId
     *                                   seen from the last attempt, else this value is null and we simply query all active mitigations for this device.
     * @param definitionToDeleteFound Flag identifying the fact whether we have already found the mitigation definition corresponding to this delete request. The reason we pass this
     *        flag is to support the edge-case where we might find the mitigation to be deleted when we try for the first time to store this workflow, but on receiving an exception,
     *        we query again, but this time, to be efficient, we only query the mitigations we did not see previously. Since the mitigation to be deleted was found previously, 
     *        it wouldn't be found again in the subsequent query.
     * @param metrics
     * @return Pair of <Long, Boolean>, where the Long value represents the max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     *         The Boolean value in the Pair represents if we saw an active mitigation definition corresponding to this delete request. If we don't find such a mitigation, we simply
     *         fill it up with the value of definitionToDeleteFound input param.
     */
    protected Pair<Long, Boolean> evaluateActiveMitigations(String deviceName, String deviceScope, String mitigationName, String mitigationTemplate, int mitigationVersion, 
                                                            Long maxWorkflowIdOnLastAttempt, boolean definitionToDeleteFound, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("evaluateActiveMitigations");
        Long maxWorkflowId = null;
        Pair<Long, Boolean> activeMitigationsEvalResult = Pair.from(null, false);
        try {
            Set<String> attributesToGet = generateAttributesToGet();
            
            // Index to use is null if we query by DeviceName + WorkflowId - since we then use the primary key of the table.
            // If we query using DeviceName + UpdateWorkfowId - we set indexToUse to the name of LSI corresponding to these keys.
            // On the first attempt to evaluate active mitigations, the maxWorkflowIdOnLastAttempt is null, so we would simply query for all active mitigations 
            // for the device (using DeviceName + UpdateWorkfowId).
            // On subsequent attempts, we would constraint our query only to the workflowIds >= maxWorkflowIdOnLastAttempt (thus using the primary key on the table).
            String indexToUse = null;
            Map<String, Condition> keyConditions = null;
            if (maxWorkflowIdOnLastAttempt == null) {
                keyConditions = getKeysForActiveMitigationsForDevice(deviceName);
                indexToUse = DDBBasedRequestStorageHandler.UNEDITED_MITIGATIONS_LSI_NAME;
            } else {
                keyConditions = getKeysForDeviceAndWorkflowId(deviceName, maxWorkflowIdOnLastAttempt);
            }

            Map<String, AttributeValue> lastEvaluatedKey = null;

            QueryResult result = getActiveMitigationsForDevice(deviceName, attributesToGet, keyConditions, lastEvaluatedKey, indexToUse, subMetrics);
            subMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOR_DEVICE, result.getCount());

            if (result.getCount() > 0) {
                activeMitigationsEvalResult = evaluateActiveMitigationsForDDBQueryResult(deviceName, deviceScope, result, mitigationName, mitigationTemplate, 
                                                                                         mitigationVersion, definitionToDeleteFound, subMetrics);
            } else {
                return Pair.from(maxWorkflowId, definitionToDeleteFound);
            }

            lastEvaluatedKey = result.getLastEvaluatedKey();
            while (lastEvaluatedKey != null) {
                result = getActiveMitigationsForDevice(deviceName, attributesToGet, keyConditions, lastEvaluatedKey, indexToUse, subMetrics);
                subMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOR_DEVICE, result.getCount());

                if (result.getCount() > 0) {
                    activeMitigationsEvalResult = evaluateActiveMitigationsForDDBQueryResult(deviceName, deviceScope, result, mitigationName, mitigationTemplate, 
                                                                                             mitigationVersion, definitionToDeleteFound, subMetrics);
                }
                lastEvaluatedKey = result.getLastEvaluatedKey();
            }
            return activeMitigationsEvalResult;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Generate a set of attributes to get from the DDB Query.
     * @return Set of string representing the attributes that need to be fetched form the DDB Query.
     */
    private Set<String> generateAttributesToGet() {
        Set<String> attributesToGet = Sets.newHashSet(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY,
                                                      DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, 
                                                      DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY, DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 
                                                      DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY);
        return attributesToGet;
    }
    
    /**
     * From the QueryResult iterate through the mitigations for this device, skip the inactive one and ones that don't match the 
     * deviceScope. For the active ones, track the workflowId of the existing mitigation - so at the end of the iteration we return 
     * back the max of the workflowIds for the currently deployed mitigations. Also check the active mitigation to locate the mitigation that we wish to be deleted.
     * We throw an exception if we find this is a duplicate delete request. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope Scope for the device on which the new mitigation is to be created. 
     * @param result QueryResult from the DDB query issued previously to find the existing mitigations for the device where the new mitigation is to be deployed.
     * @param mitigationNameToDelete Name of the new mitigation being deleted.
     * @param templateForMitigationToDelete Template used for the new mitigation being created.
     * @param versionToDelete Version of the mitigation to be deleted. If we find another active version of the same mitigation, then we throw an exception since there 
     *        must be only 1 active version of a mitigation and it is possible that the mitigation this client is requesting to be deleted has been updated by another client.
     * @param foundMitigationToDelete Flag identifying the fact whether we have already found the mitigation definition corresponding to this delete request.
     * @param metrics
     * @return Pair of <Long, Boolean>, where the Long value represents the max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     *         The Boolean value in the Pair represents if we saw an active mitigation definition corresponding to this delete request. If we don't find such a mitigation, we simply
     *         fill it up with the value of foundMitigationToDelete input param.
     */
    protected Pair<Long, Boolean> evaluateActiveMitigationsForDDBQueryResult(String deviceName, String deviceScope, QueryResult result, String mitigationNameToDelete, 
                                                                             String templateForMitigationToDelete, int versionToDelete, boolean foundMitigationToDelete, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("evaluateActiveMitigationsForDDBQueryResult");
        try {
            Long maxWorkflowId = null;
            for (Map<String, AttributeValue> item : result.getItems()) {
                // Check if this existing mitigation was for the same scope as the new request.
                String existingMitigationDeviceScope = item.get(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY).getS();
                if (!existingMitigationDeviceScope.equals(deviceScope)) {
                    continue;
                }
                
                long existingMitigationWorkflowId = Long.parseLong(item.get(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY).getN());
                if (maxWorkflowId == null) {
                    maxWorkflowId = existingMitigationWorkflowId;
                } else {
                    maxWorkflowId = Math.max(maxWorkflowId, existingMitigationWorkflowId);
                }
                
                // Check if the existing mitigation workflow was later updated by another request. If so, we don't check this mitigation workflow,
                // since the workflow which updated it would also be part of our query result.
                if (item.containsKey(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY)) {
                    Long updateWorkflowId = Long.parseLong(item.get(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY).getN());
                    if ((updateWorkflowId != null) && (updateWorkflowId > UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS)) {
                        continue;
                    }
                }
                
                // If this existing mitigation workflow failed to complete, then we don't bother checking it.
                String existingWorkflowStatus = item.get(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY).getS();
                if (existingWorkflowStatus.equals(WorkflowStatus.FAILED)) {
                    continue;
                }
                
                String existingMitigationName = item.get(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY).getS();
                int existingMitigationVersion = Integer.parseInt(item.get(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY).getN());
                String existingMitigationTemplate = item.get(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY).getS();
                String existingRequestType = item.get(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY).getS();
                long existingWorkflowId = Long.parseLong(item.get(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY).getN());
                
                if (existingMitigationName.equals(mitigationNameToDelete)) {
                    // The delete request must be for the latest version of this mitigation.
                    if (existingMitigationVersion > versionToDelete) {
                        String msg = "Found an active version (" + existingMitigationVersion + ") for mitigation: " + mitigationNameToDelete + " greater than the version to delete: " + 
                                     versionToDelete + " for device: " + deviceName + " in deviceScope: " + deviceScope + " corresponding to template: " + templateForMitigationToDelete;
                        LOG.warn(msg);
                        throw new IllegalArgumentException(msg);
                    }
                    
                    // There must be only 1 mitigation with the same name for a device, regardless of which template they were created with.
                    if (!existingMitigationTemplate.equals(templateForMitigationToDelete)) {
                        String msg = "Found an active mitigation: " + mitigationNameToDelete + " but for template: " + existingMitigationTemplate + " instead of the template: " + 
                                     templateForMitigationToDelete + " passed in the request for device: " + deviceName + " in deviceScope: " + deviceScope;
                        LOG.warn(msg);
                        throw new IllegalArgumentException(msg);
                    }
                    
                    // If we notice an existing delete request for the same mitigationName and version, then throw back an exception.
                    if (existingRequestType.equals(RequestType.DeleteRequest.name())) {
                        String msg = "Found an existing delete workflow with workflowId: " + existingWorkflowId + "for mitigation: " + mitigationNameToDelete + " when requesting delete for version: " + 
                                         versionToDelete + " for device: " + deviceName + " in deviceScope: " + deviceScope + " corresponding to template: " + templateForMitigationToDelete;
                        LOG.warn(msg);
                        throw new IllegalArgumentException(msg);
                    }
                    
                    foundMitigationToDelete = true;
                }
            }
            return Pair.from(maxWorkflowId, foundMitigationToDelete);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper for returning the milliseconds to sleep between retries when storing the delete request.
     * @return The milliseconds multiplier to determine the millis to sleep between retries.
     */
    protected long getSleepMillisMultiplierBetweenStoreRetries() {
        return NEW_WORKFLOW_ID_RETRY_SLEEP_MILLIS_MULTIPLIER;
    }
}
