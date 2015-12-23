package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;

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
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBRequestSerializer.RequestSummary;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

/**
 * DDBBasedDeleteRequestStorageHandler is responsible for persisting delete requests into DDB.
 * As part of persisting this request, based on the existing requests in the table, this handler also determines the workflowId to be used for the current request.
 */
public class DDBBasedDeleteRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedDeleteRequestStorageHandler.class);
    
    // Num Attempts to get new workflowId + retry sleep configs.
    public static final int DDB_ACTIVITY_MAX_ATTEMPTS = 10;
    
    // Prefix tags for logging warnings to be monitored.
    public static final String DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[DELETE_REQUEST_STORAGE_FAILED] ";
    
    // Keys for TSDMetric property.
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
     * @return RequestStorageResponse, include workflowId and new mitigation version that this request was stored with, using the algorithm above.
     */
    @Override
    public RequestStorageResponse storeRequestForWorkflow(@NonNull MitigationModificationRequest request, @NonNull Set<String> locations, @NonNull TSDMetrics metrics) {
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
            RequestSummary latestRequestSummary = null;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (true) {
                prevMaxWorkflowId = currMaxWorkflowId;
                
                // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
                currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, subMetrics);
                
                // Evaluate the currently active mitigations to get back a boolean flag indicating if the mitigation to delete exists.
                // The evaluation will also throw a DuplicateRequestException in case it finds that this delete request is a duplicate.
                latestRequestSummary = evaluateActiveMitigations(deviceName, deviceScope, mitigationName,
                        mitigationTemplate, mitigationVersion, prevMaxWorkflowId, latestRequestSummary, subMetrics);
                
                long newWorkflowId = 0;
                // If we didn't get any active workflows for the same deviceName and deviceScope or if we didn't find any mitigation corresponding to our delete request, throw back an exception.
                if ((currMaxWorkflowId == null) || (latestRequestSummary == null)) {
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
                    // Note: Unlike other requests we store the delete with a version one higher than the passed in version
                    // as the passed in version is the version of the mitigation to delete
                    storeRequestInDDB(
                            deleteRequest, null, locations, deviceNameAndScope, newWorkflowId, RequestType.DeleteRequest, 
                            deleteRequest.getMitigationVersion() + 1, subMetrics);
                    return new RequestStorageResponse(newWorkflowId, deleteRequest.getMitigationVersion() + 1);
                } catch (ConditionalCheckFailedException ex) {
                    String baseMsg = "Another process created workflow " + newWorkflowId + " first for " + deviceName;
                    if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                        LOG.warn(baseMsg + ". Attempt: " + numAttempts);
                    } else {
                        LOG.warn(DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX + " - " + baseMsg + ". Giving up after " + numAttempts + " attempts to store " + 
                                RequestType.DeleteRequest + ": " + ReflectionToStringBuilder.toString(request));
                        throw ex;
                    }
                } catch (AmazonClientException ex) {
                    String msg = DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX + " - Caught \"" + ex.toString()  + "\" when storing " + RequestType.DeleteRequest + " in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(request);
                    LOG.warn(msg);
                    throw ex;
                }
            }
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
     * @param versionToDelete Version of the mitigation that the client requests to be deleted. This helps us ensure the client has latest view of the mitigation to be deleted.
     * @param maxWorkflowIdOnLastAttempt If we had queried this DDB before, we could query for mitigations greaterThanOrEqual to this maxWorkflowId
     *                                   seen from the last attempt, else this value is null and we simply query all active mitigations for this device.
     * @param definitionToDeleteFound Flag identifying the fact whether we have already found the mitigation definition corresponding to this delete request. The reason we pass this
     *        flag is to support the edge-case where we might find the mitigation to be deleted when we try for the first time to store this workflow, but on receiving an exception,
     *        we query again, but this time, to be efficient, we only query the mitigations we did not see previously. Since the mitigation to be deleted was found previously, 
     *        it wouldn't be found again in the subsequent query.
     * @param metrics
     * @return the summary of the request to be deleted
     */
    protected RequestSummary evaluateActiveMitigations(
            String deviceName, String deviceScope, String mitigationName, String mitigationTemplate, 
            int versionToDelete, Long maxWorkflowIdOnLastAttempt, RequestSummary latestFromLastAttempt, 
            TSDMetrics metrics) 
    {
        TSDMetrics subMetrics = metrics.newSubMetrics("evaluateActiveMitigations");
        try {
            RequestSummary latestRequestSummary = getLatestRequestSummary(
                    deviceName, deviceScope, mitigationName, latestFromLastAttempt, subMetrics);
            
            if (latestRequestSummary == null) {
                return null;
            }
            
            if (latestRequestSummary.getMitigationVersion() > versionToDelete) {
                String msg = "Found an active version (" + latestRequestSummary.getMitigationVersion() + ") for mitigation: " + 
                        mitigationName + " greater than the version to delete: " + versionToDelete + " for device: " + deviceName + 
                        " in deviceScope: " + deviceScope + " corresponding to template: " + mitigationTemplate;
                LOG.warn(msg);
                throw new StaleRequestException400(msg);
            } else if (latestRequestSummary.getMitigationVersion() < versionToDelete) {
                String msg = "Delete was requested for version " + versionToDelete + " of " + mitigationName + 
                        " which is newer than the latest mitigation version of " + latestRequestSummary.getMitigationVersion();
                LOG.warn(msg);
                throw new StaleRequestException400(msg);
            }
            
            // There must be only 1 mitigation with the same name for a device, regardless of which template they were created with.
            if (!latestRequestSummary.getMitigationTemplate().equals(mitigationTemplate)) {
                String msg = "Found an active mitigation: " + mitigationName + " but for template: " +
                        latestRequestSummary.getMitigationTemplate() + " instead of the template: " + 
                        mitigationTemplate + " passed in the request for device: " + deviceName + 
                        " in deviceScope: " + deviceScope;
                LOG.warn(msg);
                throw new IllegalArgumentException(msg);
            }

            // If we notice an existing delete request for the same mitigationName and version, then throw back an exception.
            if (latestRequestSummary.getRequestType().equals(RequestType.DeleteRequest.name())) {
                String status = latestRequestSummary.getWorkflowStatus();
                if (status.equals(WorkflowStatus.PARTIAL_SUCCESS) || status.equals(WorkflowStatus.INDETERMINATE)) {
                    // Allow retrying deletes
                    LOG.info("Found an existing delete request with jobId: " + latestRequestSummary.getWorkflowId() + " for mitigation: " + 
                            mitigationName + " for device: " + deviceName + " in deviceScope: " + deviceScope + 
                            " corresponding to template: " + mitigationTemplate + " whose status is: " +
                             status + ". Hence not considering this existing delete request as a duplicate.");
                } else {
                    String msg = "Found an existing delete request with jobId: " + latestRequestSummary.getWorkflowId() + 
                                " for mitigation: " + mitigationName + " when requesting delete for " +
                                "version: " + versionToDelete + " for device: " + deviceName + 
                                " in deviceScope: " + deviceScope + " corresponding to template: " + mitigationTemplate;
                    LOG.warn(msg);
                    throw new DuplicateRequestException400(msg);
                }
            }
            
            return latestRequestSummary;
        } finally {
            subMetrics.end();
        }
    }
}
