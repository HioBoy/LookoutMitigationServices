package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import lombok.NonNull;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Sets;
import com.google.common.collect.Lists;

/**
 * DDBBasedDeleteRequestStorageHandler is responsible for persisting delete requests into DDB.
 * As part of persisting this request, based on the existing requests in the table, this handler also determines the workflowId to be used for the current request.
 */
public class DDBBasedDeleteRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedDeleteRequestStorageHandler.class);
    
    // Num Attempts to get new workflowId + retry sleep configs.
    public static final int DDB_ACTIVITY_MAX_ATTEMPTS = 10;
    private static final int DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    // Prefix tags for logging warnings to be monitored.
    public static final String DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[DELETE_REQUEST_STORAGE_FAILED] ";
    
    // Keys for TSDMetric property.
    private static final String NUM_ACTIVE_INSTANCES_FOR_MITIGATIONS = "NumActiveMitigationInstances";
    private static final String NUM_ATTEMPTS_TO_STORE_DELETE_REQUEST = "NumDeleteRequestStoreAttempts";
    
    public DDBBasedDeleteRequestStorageHandler(AmazonDynamoDBClient dynamoDBClient, String domain) {
        super(dynamoDBClient, domain);
    }

    /**
     * Stores the delete request into the DDB Table. While storing, it identifies the new workflowId to be associated with this request and returns back the same.
     * The algorithm it uses to identify the workflowId to use is:
     * 1. Identify the deviceNameAndScope that corresponds to this request.
     * 2. Query for all active mitigations for this device.
     * 3. Get the max workflowId from all the mitigations that currently exists for this device.
     * 4. Evaluate the currently active mitigations to ensure this is not a duplicate request and also that the mitigation to delete actually exists.
     * 5. If no active mitigations exist for this device throw back an exception since there is nothing to delete.
     * 6. If when storing the request we encounter an exception, it could be either because someone else started using the maxWorkflowId+1 workflowId for that device
     *    or it is some transient exception. In either case, we query the DDB table once again for mitigations >= maxWorkflowId for the device and
     *    continue with step 3. 
     * @param request Request to be stored.
     * @param locations Set of String where this request applies.
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@NonNull MitigationModificationRequest request, @NonNull Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);

        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedDeleteRequestStorageHandler.storeRequestForWorkflow");
        int numAttempts = 0;
        try {
            DeleteMitigationFromAllLocationsRequest deleteRequest = (DeleteMitigationFromAllLocationsRequest) request;
            
            String mitigationName = deleteRequest.getMitigationName();
            String mitigationTemplate = deleteRequest.getMitigationTemplate();
            int mitigationVersion = deleteRequest.getMitigationVersion();

            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);

            String deviceName = deviceNameAndScope.getDeviceName().name();
            String deviceScope = deviceNameAndScope.getDeviceScope().name();

            Long prevMaxWorkflowId = null;
            Long currMaxWorkflowId = null;
            boolean activeMitigationToDeleteFound = false;
            
            // Holds a reference to the last caught exception, to report that back if all retries fail.
            Throwable lastCaughtException = null;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts++ < DDB_ACTIVITY_MAX_ATTEMPTS) {
                prevMaxWorkflowId = currMaxWorkflowId;
                
                // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
                currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, subMetrics);
                
                // Evaluate the currently active mitigations to get back a boolean flag indicating if the mitigation to delete exists.
                // The evaluation will also throw a DuplicateRequestException in case it finds that this delete request is a duplicate.
                activeMitigationToDeleteFound = evaluateActiveMitigations(deviceName, deviceScope, mitigationName, mitigationTemplate, mitigationVersion, 
                                                                          prevMaxWorkflowId, activeMitigationToDeleteFound, subMetrics);
                
                long newWorkflowId = 0;
                // If we didn't get any active workflows for the same deviceName and deviceScope or if we didn't find any mitigation corresponding to our delete request, throw back an exception.
                if ((currMaxWorkflowId == null) || !activeMitigationToDeleteFound) {
                    String msg = "No active mitigation to delete found when querying for deviceName: " + deviceName + " deviceScope: " + deviceScope + 
                                 ", for request: " + ReflectionToStringBuilder.toString(deleteRequest);
                    LOG.warn(msg);
                    throw new MissingMitigationException400(msg);
                } else {
                    // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                    newWorkflowId = currMaxWorkflowId + 1;
                    sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);
                }

                try {
                    storeRequestInDDB(deleteRequest, locations, deviceNameAndScope, newWorkflowId, RequestType.DeleteRequest, deleteRequest.getMitigationVersion(), subMetrics);
                    return newWorkflowId;
                } catch (Exception ex) {
                    lastCaughtException = ex;
                    
                    String msg = "Caught exception when storing delete request in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(deleteRequest);
                    LOG.warn(msg, ex);
                }
                
                if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    try {
                        Thread.sleep(getSleepMillisMultiplierBetweenStoreRetries() * numAttempts);
                    } catch (InterruptedException ignored) {}
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX + "- Unable to store delete request : " + ReflectionToStringBuilder.toString(deleteRequest) +
                         " after " + numAttempts + " attempts";
            LOG.warn(msg, lastCaughtException);
            throw new RuntimeException(msg, lastCaughtException);
        } finally {
            subMetrics.addCount(NUM_ATTEMPTS_TO_STORE_DELETE_REQUEST, numAttempts);
            subMetrics.end();
        }
    }
    
    /**
     * Evaluate the active mitigations currently in place for this device and scope - to check if the mitigation that needs to be deleted is currently 
     * active and also ensure that this isn't a duplicate delete request. Protected for unit-testing.
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
     * @return boolean flag, which represents if we saw an active mitigation definition corresponding to this delete request.
     */
    protected boolean evaluateActiveMitigations(String deviceName, String deviceScope, String mitigationName, String mitigationTemplate, int mitigationVersion, 
                                                Long maxWorkflowIdOnLastAttempt, boolean definitionToDeleteFound, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("evaluateActiveMitigations");
        try {
            Set<String> attributesToGet = generateAttributesToGet();
            
            Map<String, Condition> queryFiltersForMitigationExistenceCheck = createQueryFiltersToCheckDeleteMitigationExists(mitigationName);
            
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
                
                // If we are querying by the primary key (hash+range) we also add a filter to skip the updated workflows.
                AttributeValue attrVal = new AttributeValue().withN("0");
                Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(Arrays.asList(attrVal));
                queryFiltersForMitigationExistenceCheck.put(UPDATE_WORKFLOW_ID_KEY, condition);
            }

            Map<String, AttributeValue> lastEvaluatedKey = null;
            do {
                QueryResult result = getActiveMitigationsForDevice(deviceName, deviceScope, attributesToGet, keyConditions, 
                                                                   lastEvaluatedKey, indexToUse, queryFiltersForMitigationExistenceCheck, subMetrics);
                subMetrics.addCount(NUM_ACTIVE_INSTANCES_FOR_MITIGATIONS, result.getCount());
                
                if (result.getCount() > 0) {
                    boolean definitionToDeleteFoundInQueryResult = evaluateActiveMitigationsForDDBQueryResult(deviceName, deviceScope, result, mitigationName, 
                                                                                                              mitigationTemplate, mitigationVersion, subMetrics);
                    
                    // If we found the definition to delete then set the definitionToDeleteFound to true.
                    if (definitionToDeleteFoundInQueryResult) {
                        definitionToDeleteFound = true;
                    }
                }

                lastEvaluatedKey = result.getLastEvaluatedKey();
            } while(lastEvaluatedKey != null);
            return definitionToDeleteFound;
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
     * From the QueryResult iterate through the mitigations for this device, check the active mitigations to locate the mitigation that we wish to be deleted.
     * We throw an exception if we find this is a duplicate delete request. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope Scope for the device on which the new mitigation is to be created. 
     * @param result QueryResult from the DDB query issued previously to find the existing mitigations for the device where the new mitigation is to be deployed.
     * @param mitigationNameToDelete Name of the new mitigation being deleted.
     * @param templateForMitigationToDelete Template used for the new mitigation being created.
     * @param versionToDelete Version of the mitigation to be deleted. If we find another active version of the same mitigation, then we throw an exception since there 
     *        must be only 1 active version of a mitigation and it is possible that the mitigation this client is requesting to be deleted has been updated by another client.
     * @param metrics
     * @return Pair of <Long, Boolean>, where the Long value represents the max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     *         The Boolean value in the Pair represents if we saw an active mitigation definition corresponding to this delete request. If we don't find such a mitigation, we simply
     *         fill it up with the value of foundMitigationToDelete input param.
     */
    protected boolean evaluateActiveMitigationsForDDBQueryResult(String deviceName, String deviceScope, QueryResult result, String mitigationNameToDelete, 
                                                                 String templateForMitigationToDelete, int versionToDelete, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("evaluateActiveMitigationsForDDBQueryResult");
        try {
            boolean foundMitigationToDelete = false;
            
            // Keep a track of the DDB Items which might be potential duplicate delete requests.
            List<Map<String, AttributeValue>> potentialDuplicateDeletes = Lists.newArrayList();
            
            // Keep a track of the previous (create/edit) request that resulted in this mitigation's existance, which the current delete request will be deleting.
            long requestBeingDeleted = -1;

            for (Map<String, AttributeValue> item : result.getItems()) {
                long existingMitigationWorkflowId = Long.parseLong(item.get(WORKFLOW_ID_KEY).getN());
                int existingMitigationVersion = Integer.parseInt(item.get(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY).getN());
                String existingMitigationTemplate = item.get(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY).getS();
                String existingRequestType = item.get(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY).getS();
                
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
                   String existingRequestWorkflowStatus = item.get(WORKFLOW_STATUS_KEY).getS();
                    if (existingRequestWorkflowStatus.equals(WorkflowStatus.PARTIAL_SUCCESS) || existingRequestWorkflowStatus.equals(WorkflowStatus.INDETERMINATE)) {
                        LOG.info("Found an existing delete request with jobId: " + existingMitigationWorkflowId + "for mitigation: " + mitigationNameToDelete + " for device: " +
                                 deviceName + " in deviceScope: " + deviceScope + " corresponding to template: " + templateForMitigationToDelete + " whose status is: " +
                                 existingRequestWorkflowStatus + ". Hence not considering this existing delete request as a duplicate.");
                    }
                    potentialDuplicateDeletes.add(item);
                    continue;
                }
                
                if (requestBeingDeleted > -1) {
                    LOG.error("Found more than 1 instance of the mitigation: " + mitigationNameToDelete + " for deviceName: " + deviceName + " and deviceScope: " + deviceScope +
                              " and template: " + templateForMitigationToDelete + " to be deleted by this request. Previous request: " + 
                              requestBeingDeleted + ", found other request: " + existingMitigationWorkflowId + ". Continuing anyway by considering the latest workflowId as the request being deleted.");
                }

                // Track the latest request which caused this mitigation to exist, which will be deleted by this delete request.
                if (requestBeingDeleted < existingMitigationWorkflowId) {
                    requestBeingDeleted = existingMitigationWorkflowId;
                    foundMitigationToDelete = true;
                }
            }

            // If no mitigation is found that needs to be deleted, return false right here.
            if (!foundMitigationToDelete) {
                return false;
            }

            // Go through the potential delete requests prior to the current one and check if any of them have a workflowId greater than the workflowId which corresponds to the creation
            // of the mitigation that needs to be deleted. For example: Delete#1 has Id 1, Create#1 has Id 2, Delete#2 has Id 3. In this case, the requestBeingDeleted is Id 2.
            // The Delete#1 has a lower Id than the requestBeingDeleted and hence not considered a duplicate. But Delete#2 has Id 3 > requestBeingDeleted and hence is considered as a duplicate.
            for (Map<String, AttributeValue> item : potentialDuplicateDeletes) {
                long previousDeleteWorkflowId = Long.parseLong(item.get(WORKFLOW_ID_KEY).getN());
                if (previousDeleteWorkflowId > requestBeingDeleted) {
                    String msg = "Found an existing delete request with jobId: " + previousDeleteWorkflowId + "for mitigation: " + mitigationNameToDelete + " when requesting delete for " +
                                 "version: " + versionToDelete + " for device: " + deviceName + " in deviceScope: " + deviceScope + " corresponding to template: " + templateForMitigationToDelete;
                    LOG.warn(msg);
                    throw new DuplicateRequestException400(msg);
                }
            }
            return true;
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper for returning the milliseconds to sleep between retries when storing the delete request.
     * @return The milliseconds multiplier to determine the millis to sleep between retries.
     */
    protected long getSleepMillisMultiplierBetweenStoreRetries() {
        return DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER;
    }
    
    /**
     * Helper to create queryFilters to be used for checking if the mitigation to delete exists.
     * @param mitigationName Name of the mitigation that needs to be deleted.
     * @return Map <String, Condition>, representing the queryFilters to use when querying DDB.
     */
    private Map<String, Condition> createQueryFiltersToCheckDeleteMitigationExists(String mitigationName) {
        Map<String, Condition> queryFilters = new HashMap<>();
        
        AttributeValue attrVal = new AttributeValue(WorkflowStatus.FAILED);
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.NE);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(WORKFLOW_STATUS_KEY, condition);
        
        attrVal = new AttributeValue(mitigationName);
        condition = new Condition().withComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(MITIGATION_NAME_KEY, condition);
        
        return queryFilters;
    }
}
