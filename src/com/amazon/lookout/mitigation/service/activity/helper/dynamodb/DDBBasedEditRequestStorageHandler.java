package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToFixedActionMapper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.util.concurrent.Uninterruptibles;

import javax.annotation.Nonnull;

public class DDBBasedEditRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedEditRequestStorageHandler.class);
    
    private static final String FAILED_TO_STORE_EDIT_REQUEST_KEY = "FailedToStoreEditRequest";
    private static final String INVALID_REQUEST_TO_STORE_KEY = "InvalidRequestToStore";
    
    // Num Attempts + Retry Sleep Configs.
    private static final int DDB_ACTIVITY_MAX_ATTEMPTS = 10;
    private static final int DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;

    // Prefix tags for logging warnings to be monitored.
    private static final String EDIT_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[EDIT_REQUEST_STORAGE_FAILED]";

    // Keys for TSDMetric property.
    private static final String NUM_ATTEMPTS_TO_STORE_EDIT_REQUEST = "NumEditRequestStoreAttempts";
    
    private final DataConverter jsonDataConverter = new JsonDataConverter();
    private final TemplateBasedRequestValidator templateBasedRequestValidator;

    public DDBBasedEditRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain, @Nonnull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain);
        this.templateBasedRequestValidator = templateBasedRequestValidator;
    }

    /**
     * Stores the edit request into the DDB Table. While storing, it identifies the new workflowId to be associated with this request and returns back the same.
     * The algorithm it uses to identify the workflowId to use is:
     * 1. Identify the max workflowId for the active mitigations for this device and scope and try to use this maxWorkflowId+1 as the new workflowId
     *    when storing the request.
     * 2. Identify the latest version of mitigation with the same mitigation name.
     * 3. Make sure the mitigation template, service name stay the same (with the previous version if there is any) and mitigation definitions are different.
     * 4. Make sure the mitigation version in request is higher than the latest version.
     * 5. If when storing the request we encounter an exception, it could be either because someone else started using the maxWorkflowId+1 workflowId for that device
     *    or it is some transient exception. In either case, we query the DDB table once again for mitigations >= maxWorkflowId for the device and
     *    continue with step 2.
     *   
     * @param request Request to be stored.
     * @param locations Set of String representing the locations where this request applies.
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@NonNull MitigationModificationRequest request, Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);
        
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedEditRequestStorageHandler.storeRequestForWorkflow")) {
            subMetrics.addZero(INVALID_REQUEST_TO_STORE_KEY);
            subMetrics.addZero(FAILED_TO_STORE_EDIT_REQUEST_KEY);
            
            if (!(request instanceof EditMitigationRequest)) {
                subMetrics.addOne(INVALID_REQUEST_TO_STORE_KEY);
                String msg = "Requested EditRequestStorageHandler to store a non-edit request";
                LOG.error(msg + ", for request: " + request + " in locations: " + locations);
                throw new IllegalArgumentException(msg);
            }
            EditMitigationRequest editMitigationRequest = (EditMitigationRequest) request;
        
            String mitigationName = editMitigationRequest.getMitigationName();
            subMetrics.addProperty("MitigationName", mitigationName);
            
            String mitigationTemplate = editMitigationRequest.getMitigationTemplate();
            subMetrics.addProperty("MitigationTemplate", mitigationTemplate);
            
            Integer mitigationVersion = editMitigationRequest.getMitigationVersion();
            subMetrics.addProperty("MitigationVersion", String.valueOf(mitigationVersion));
            
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            String deviceName = deviceNameAndScope.getDeviceName().name();
            subMetrics.addProperty("DeviceName", deviceName);
            
            String deviceScope = deviceNameAndScope.getDeviceScope().name();
            subMetrics.addProperty("DeviceScope", deviceScope);

            MitigationDefinition mitigationDefinition = editMitigationRequest.getMitigationDefinition();
            
            // If action is null, check with the MitigationTemplateToFixedActionMapper to get the action for this template.
            // This is required to get the action stored with the mitigation definition in DDB, allowing it to be later displayed on the UI.
            if (mitigationDefinition.getAction() == null) {
                ActionType actionType = MitigationTemplateToFixedActionMapper.getActionTypesForTemplate(mitigationTemplate);
                if (actionType == null) {
                    String msg = "Validation for this request went through successfully, but this request doesn't have any action associated with it and no " +
                                 "mapping found in the MitigationTemplateToFixedActionMapper for the template in this request. Request: " + ReflectionToStringBuilder.toString(editMitigationRequest);
                    LOG.error(msg);
                    throw new RuntimeException(msg);
                }
                mitigationDefinition.setAction(actionType);
            }

            Long prevMaxWorkflowId = null;
            Long currMaxWorkflowId = null;
            Integer prevLatestMitigationVersion = null;
            Integer currLatestMitigationVersion = null;
            
            int numAttempts = 0;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                numAttempts++;
                subMetrics.addOne(NUM_ATTEMPTS_TO_STORE_EDIT_REQUEST);
                prevMaxWorkflowId = currMaxWorkflowId;
                
                // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
                currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, subMetrics);
                
                // Next, retrieve the current latest mitigation version for the same mitigation on the same device+scope
                prevLatestMitigationVersion = currLatestMitigationVersion;
                currLatestMitigationVersion = getLatestVersionForMitigationOnDevice(deviceName, deviceScope, mitigationName, prevLatestMitigationVersion, subMetrics);
                
                long newWorkflowId = 0;
                // If we didn't get any version for the same deviceName, deviceScope and mitigation name, throw IllegalArgumentException.
                if (currLatestMitigationVersion == null || currMaxWorkflowId == null) {
                    String msg = "No existing mitigation found in DDB for deviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " and mitigationName: " + mitigationName + ".";
                    LOG.info(msg + " For request: " + ReflectionToStringBuilder.toString(editMitigationRequest));
                    throw new IllegalArgumentException(msg);
                } 
                
                if (mitigationVersion <= currLatestMitigationVersion) {
                    String msg = "Mitigation found in DDB for deviceName: " + deviceName + " and deviceScope: " + deviceScope + " and mitigationName: " + mitigationName + 
                                 " has a newer version: " + currLatestMitigationVersion + " than one in request: " + mitigationVersion + ". For request: " + ReflectionToStringBuilder.toString(editMitigationRequest);
                    LOG.info(msg);
                    throw new StaleRequestException400(msg);
                }

                final int expectedMitigationVersion = currLatestMitigationVersion + 1;
                if (mitigationVersion != expectedMitigationVersion) {
                    String msg = "Unexpected mitigation version in request for deviceName: " + deviceName + " and deviceScope: " + deviceScope +
                                 " and mitigationName: " + mitigationName + " Expected mitigation version: " + expectedMitigationVersion +
                                 " , but mitigation version in request was: " + mitigationVersion + ". Request: " + ReflectionToStringBuilder.toString(editMitigationRequest);
                    LOG.info(msg);
                    throw new IllegalArgumentException();
                }
              
                // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                newWorkflowId = currMaxWorkflowId + 1;
                sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);

                try {
                    storeRequestInDDB(editMitigationRequest, locations, deviceNameAndScope, newWorkflowId, RequestType.EditRequest, mitigationVersion, subMetrics);
                    return newWorkflowId;
                } catch (Exception ex) {
                    String msg = "Caught exception when storing edit request in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(editMitigationRequest);
                    LOG.warn(msg);
                }
                
                if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    Uninterruptibles.sleepUninterruptibly(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts, TimeUnit.MILLISECONDS);
                }
            }
            
            subMetrics.addOne(FAILED_TO_STORE_EDIT_REQUEST_KEY);
            String msg = EDIT_REQUEST_STORAGE_FAILED_LOG_PREFIX + " - Unable to store edit request : " + ReflectionToStringBuilder.toString(editMitigationRequest) +
                         " after " + numAttempts + " attempts";
            LOG.warn(msg);
            throw new RuntimeException(msg);
        }
    }
    
    /**
     * Helper to return a JSONDataConverter. We use the data converter provided by the SWF dependency.
     * Protected for unit-testing.
     * @return Instance of JSONDataConverter.
     */
    protected DataConverter getJSONDataConverter() {
        return jsonDataConverter;
    }
    
    /**
     * Helper to return a TemplateBasedRequestValidator. Protected for unit-testing.
     * @return Instance of TemplateBasedRequestValidator.
     */
    protected TemplateBasedRequestValidator getTemplateBasedValidator() {
        return templateBasedRequestValidator;
    }
}
