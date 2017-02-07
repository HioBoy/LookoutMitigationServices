package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.DuplicateMitigationNameException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBRequestSerializer.RequestSummary;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

/**
 * This class contains common functions used by for creating and editing mitigations.
 * 
 * @author stevenso
 *
 */
public class DDBBasedCreateAndEditRequestStorageHandler extends DDBBasedRequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedCreateAndEditRequestStorageHandler.class);
    
    protected static final String UPDATE_REQUEST_STORAGE_FAILED_LOGSCAN_TOKEN = "[UPDATE_REQUEST_STORAGE_FAILED]";
    
    // Keys for TSDMetrics.
    private static final String NUM_CREATE_OR_UPDATE_REQUEST_ATTEMPTS = "NumDDBCreateOrUpdateRequestStoreAttempts";
    
    private final TemplateBasedRequestValidator templateValidator;

    public DDBBasedCreateAndEditRequestStorageHandler(
            AmazonDynamoDB dynamoDBClient, String domain, TemplateBasedRequestValidator templateValidator) 
    {
        super(dynamoDBClient, domain);
        this.templateValidator = templateValidator;
    }

    /**
     * Store the request for a workflow.
     * 
     * @param deviceNameAndScope
     * @param requestType
     * @param request
     * @param locations
     * @param definition
     * @param version
     * @param validator a validator to validate the input before 
     * @param metrics
     * @return RequestStorageResponse includes the workflow id and mitigation version of the for the new request
     * 
     * @throws AmazonClientException if an attempt to read or write to DynamoDB failed too many times
     * @throws MissingMitigationException400 if this is not a create request and the mitigation doesn't exist
     *              or has been deleted for edit request.(allow rollback on deleted mitigation)
     * @throws DuplicateMitigationNameException400 on create if the mitigation already exists
     * @throws DuplicateDefinitionException400 if a conflicting mitigation already exists. The definition of conflicting 
     *   depends on the template type
     * @throws StaleRequestException400 if the version of the request in DDB isn't the version specified by the request
     * @throws IllegalArgumentException if the modification is invalid (e.g. the template type of the request doesn't match the existing
     *  template type)
     * @throws InternalServerError500 for unexpected errors, e.g. we've hit the max workflow id
     */
    protected RequestStorageResponse storeRequestForWorkflow(
            RequestType requestType, MitigationModificationRequest request, 
            Set<String> locations, MitigationDefinition definition, 
            int mitigationVersion, String errorTag, TSDMetrics metrics) 
        throws AmazonClientException, MissingMitigationException400, DuplicateMitigationNameException400,
               DuplicateDefinitionException400, StaleRequestException400, InternalServerError500, IllegalArgumentException
    {
        String mitigationTemplate = request.getMitigationTemplate();
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
        String deviceName = deviceNameAndScope.getDeviceName().name();
        String deviceScope = deviceNameAndScope.getDeviceScope().name();
        String mitigationName = request.getMitigationName();
        
        int numAttempts = 0;
        Long prevMaxWorkflowId = null;
        Long currMaxWorkflowId = null;
        RequestSummary latestRequestSummary = null;
        boolean isUpdate = (requestType != RequestType.CreateRequest);
        boolean isEdit = (requestType == RequestType.EditRequest);
        
        // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
        // if successful, else end the loop and throw back an exception.
        while(true) {
            numAttempts++;
            metrics.addOne(NUM_CREATE_OR_UPDATE_REQUEST_ATTEMPTS);
            prevMaxWorkflowId = currMaxWorkflowId;
            
            long newWorkflowId = 0;
            // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
            currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, metrics);
            
            // If we didn't get any workflows for the same deviceName and deviceScope, we simply assign the newWorkflowId to be the min for this deviceScope.
            if (currMaxWorkflowId == null) {
                newWorkflowId = deviceNameAndScope.getDeviceScope().getMinWorkflowId();
            } else {
                // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                newWorkflowId = currMaxWorkflowId + 1;
                sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);
            }

            // retrieve the current latest mitigation version for the same mitigation on the same device+scope
            latestRequestSummary = getLatestRequestSummary(
                    deviceName, deviceScope, mitigationName, latestRequestSummary, metrics);

            if (isUpdate) {
                // If we didn't get any version for the same deviceName, deviceScope and
                // mitigation name, throw MissingMitigationException400.
                if (latestRequestSummary == null || currMaxWorkflowId == null) {
                    String msg = "No existing mitigation found in DDB for deviceName: " + deviceName
                                 + " and deviceScope: " + deviceScope + " and mitigationName: " + mitigationName
                                 + ". For request: " + requestToString(request);
                    LOG.info(msg);
                    throw new MissingMitigationException400(msg);
                }
                
                checkMitigationVersion(
                        request, deviceNameAndScope, mitigationVersion, latestRequestSummary.getMitigationVersion());
                
                // reject edit request that is editing a deleted request
                if (latestRequestSummary.getRequestType().equals(RequestType.DeleteRequest.name()) && isEdit) {
                    String msg = "Mitigation " + mitigationName + " for deviceName: " + deviceName 
                            + " and deviceScope: " + deviceScope + " has already been deleted"
                            + ". For request: " + requestToString(request);
                    LOG.info(msg);
                    throw new MissingMitigationException400(msg);
                }
                
                if (!latestRequestSummary.getMitigationTemplate().equals(request.getMitigationTemplate())) {
                    String msg = "The template type for a mitigation cannot be changed. Mitigation " + mitigationName 
                            + " for deviceName: " + deviceName + " and deviceScope: " + deviceScope 
                            + " has template type " + latestRequestSummary.getMitigationTemplate() + " but was requested"
                            + " to change to template type " + request.getMitigationTemplate()
                            + ". For request: " + requestToString(request);
                    LOG.info(msg);
                    // TODO: Use a more specific exception
                    throw new IllegalArgumentException(msg);
                }
            } else {
                // if found an existing mitigation, validate it is successfully deleted.
                if (latestRequestSummary != null) {
                    if (RequestType.DeleteRequest.name().equals(latestRequestSummary.getRequestType())
                            && WorkflowStatus.SUCCEEDED.equals(latestRequestSummary.getWorkflowStatus())) {
                        // if the existing mitigation has been deleted successfully from the system, it is safe to
                        // re-create it again. If not, reject request
                        mitigationVersion = latestRequestSummary.getMitigationVersion() + 1;
                    } else {
                        String msg = "Mitigation " + mitigationName + " for deviceName: " + deviceName 
                                + " and deviceScope: " + deviceScope + " has already existed"
                                + ". For request: " + requestToString(request);
                        LOG.info(msg);
                        throw new DuplicateMitigationNameException400(msg);
                    }
                }
            }

            if (templateValidator.requiresCheckForDuplicateAndConflictingRequests(mitigationTemplate)) {
                // Next, check if we have any duplicate mitigations already in place.
                checkForDuplicateAndConflictingRequests(deviceName, deviceScope, definition, mitigationName, mitigationTemplate, 
                                                  isUpdate, prevMaxWorkflowId, metrics);
            }
            
            try {
                storeRequestInDDB(request, definition,
                       locations, deviceNameAndScope, newWorkflowId, requestType, mitigationVersion, metrics);
                return new RequestStorageResponse(newWorkflowId, mitigationVersion);
            } catch (ConditionalCheckFailedException ex) {
                String baseMsg = "Another process created workflow " + newWorkflowId + " first for " + deviceName;
                if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    LOG.warn(baseMsg + ". Attempt: " + numAttempts);
                    sleepForPutRetry(numAttempts);
                } else {
                    LOG.warn(errorTag + " - " + baseMsg + ". Giving up after " + numAttempts + " attempts to store " + 
                            requestType + ": " + ReflectionToStringBuilder.toString(request));
                    throw ex;
                }
            } catch (AmazonClientException ex) {
                String msg = errorTag + " - Caught \"" + ex.toString()  + "\" when storing " + requestType + " in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                             " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(request);
                LOG.warn(msg);
                throw ex;
            }
        }
    }
    
    /**
     * Query DDB and check if there exists a duplicate request for the request being processed.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope DeviceScope for the device where the new mitigation needs to be created.
     * @param mitigationDefinition Mitigation definition for the new create request.
     * @param mitigationName Name of the new mitigation being created.
     * @param mitigationTemplate Template being used for the new mitigation being created.
     * @param maxWorkflowIdOnLastAttempt the maximum workflow id found on a previous query. This is used 
     *   to avoid rechecking the same requests if we have to retry if the insert failed because of 
     *   a new request being added by another thread/process.
     * @param metrics the metrics object to use to record metrics
     * @return Max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     * 
     * @throws AmazonClientException if the attempt to read from DynamoDB failed too many times
     * @throws DuplicateDefinitionException400 if a conflicting mitigation already exists. The definition of conflicting 
     *   depends on the template type
     */
    private void checkForDuplicateAndConflictingRequests(
            String deviceName, String deviceScope, MitigationDefinition mitigationDefinition, String mitigationName, 
            String mitigationTemplate, boolean isUpdate, Long maxWorkflowIdOnLastAttempt, TSDMetrics metrics)
        throws AmazonClientException, DuplicateDefinitionException400
    {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateAndEditRequestStorageHandler.checkForDuplicateAndConflictingRequests");
        try {
            Map<String, AttributeValue> lastEvaluatedKey = null;
            
            Map<String, Condition> filterForActiveRequests = filterForActiveRequests();
            
            String indexToUse = null;
            Map<String, Condition> keyConditions = null;
            if (maxWorkflowIdOnLastAttempt == null) {
                /*
                 * On the first pass use the UNEDITED_MITIGATIONS_LSI_NAME that allows us to query
                 * by device name and UPDATE_WORKFLOW_ID_KEY so that we can view only requests
                 * that haven't been replaced by a newer version.
                 */
                keyConditions = DDBRequestSerializer.getQueryKeyForActiveMitigationsForDevice(deviceName);
                indexToUse = DDBBasedRequestStorageHandler.UNEDITED_MITIGATIONS_LSI_NAME;
            } else {
                /*
                 * If we retry we use the primary key which is device name and workflow id so that we
                 * can check only the requests newer than the ones we checked the last time. Even if that does
                 * include some requests that have been replaced (unlikely given how short the time window is)
                 * it should still be a much smaller number.
                 */
                keyConditions = DDBRequestSerializer.getPrimaryQueryKey(deviceName, maxWorkflowIdOnLastAttempt);
                
                // Filter out any requests that have been replaced
                DDBRequestSerializer.addActiveRequestCondition(filterForActiveRequests);
            }
            
            do {
                QueryResult result = null;
                try {
                    result = getActiveMitigationsForDevice(deviceName, deviceScope, null, keyConditions, 
                                                           lastEvaluatedKey, indexToUse, filterForActiveRequests, subMetrics);
                    lastEvaluatedKey = result.getLastEvaluatedKey();
                    subMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOR_DEVICE, result.getCount());
                } catch (AmazonClientException ex) {
                    String msg = "Caught exception when querying active mitigations from DDB for device: " + deviceName + " and deviceScope: " + deviceScope + 
                                 ", keyConditions: " + keyConditions + ", indexToUse: " + indexToUse;
                    LOG.warn(msg, ex);
                    throw ex;
                }

                checkForDuplicateAndConflictingRequestsInResult(
                        deviceName, result, isUpdate, mitigationDefinition, mitigationName, mitigationTemplate, subMetrics);
            } while (lastEvaluatedKey != null);
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Creates the queryFilters to use when querying DDB to check for active mitigations that could conflict
     * with the new request. This includes all requests that aren't delete requests,
     * FAILED or INDETERMINATE or marked as defunct using DEFUNCT_DATE .
     * 
     * @return map representing the queryFilter to use for the DDB query.
     */
    private static Map<String, Condition> filterForActiveRequests() {
        Map<String, Condition> queryFilters = new HashMap<>();
        
        {
            String[] workflowStatuses = WorkflowStatus.values();
            List<AttributeValue> attrValues = new ArrayList<>(workflowStatuses.length - 2);
            for (String workflowStatus : workflowStatuses) {
                if (!workflowStatus.equals(WorkflowStatus.FAILED) && !workflowStatus.equals(WorkflowStatus.INDETERMINATE)) {
                    attrValues.add(new AttributeValue(workflowStatus));
                }
            }
            Condition condition = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attrValues);
            queryFilters.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, condition);
        }
        
        {
            AttributeValue attrVal = new AttributeValue(RequestType.DeleteRequest.name());
            Condition condition = new Condition().withComparisonOperator(ComparisonOperator.NE);
            condition.setAttributeValueList(Arrays.asList(attrVal));
            queryFilters.put(MitigationRequestsModel.REQUEST_TYPE_KEY, condition);
        }
        
        {
            Condition condition = new Condition().withComparisonOperator(ComparisonOperator.NULL);
            queryFilters.put(MitigationRequestsModel.DEFUNCT_DATE, condition);
        }
        
        return queryFilters;
    }

    /**
     * From the QueryResult iterate through the mitigations for this device, check if it is a duplicate of the mitigation 
     * requested to be created. If we find a duplicate, we throw back an exception.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param isUpdate if true this is an update to an existing mitigation instead of an entirely new one
     * @param result QueryResult from the DDB query issued previously to find the existing mitigations for the device where the new mitigation is to be deployed.
     * @param newDefinition MitigationDefinition for the new mitigation that needs to be applied.
     * @param newMitigationName Name of the new mitigation being created.
     * @param newDefinitionTemplate Template used for the new mitigation being created.
     * @param metrics
     * @return Max workflowId for existing mitigations for the same deviceName and deviceScope. Null if there are no such active mitigations.
     * 
     * @throws DuplicateDefinitionException400 if a conflicting mitigation already exists. The definition of conflicting 
     *   depends on the template type
     */
    private void checkForDuplicateAndConflictingRequestsInResult(String deviceName, QueryResult result, boolean isUpdate,
                                                   MitigationDefinition newDefinition, String newMitigationName, 
                                                   String newDefinitionTemplate, TSDMetrics metrics) 
       throws DuplicateDefinitionException400
    {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateAndEditRequestStorageHandler.checkForDuplicatesFromDDBResult");
        try {
            for (Map<String, AttributeValue> item : result.getItems()) {
                MitigationRequestDescription existingDescription = DDBRequestSerializer.convertToRequestDescription(item);
                String existingMitigationName = existingDescription.getMitigationName();
                String existingMitigationTemplate = existingDescription.getMitigationTemplate();
                
                // Check if the mitigation name for the existing mitigation and the new mitigation are the same. If so, throw back an exception.
                if (existingMitigationName.equals(newMitigationName)) {
                    if (isUpdate) {
                        // Don't compare the old version with the new version of the same mitigation
                        continue;
                    } else {
                        // The dynamodb request store action will fail, as another request has already
                        // successfully create the mitigation. So the workflowId has been changed. The next
                        // retry will reject the request with DuplicateMitigationNameException400
                        return;
                    }
                }

                checkDuplicateDefinition(existingDescription.getMitigationDefinition(), existingMitigationName, existingMitigationTemplate, 
                                         newMitigationName, newDefinitionTemplate, newDefinition, subMetrics);
            }
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Delegate this to per template implementation
     *  
     * Protected for unit-testing.
     * @param existingDefinitionAsJSONString MitigationDefinition for an existing mitigation represented as a JSON string.
     * @param existingMitigationName MitigationName for the existing mitigation.
     * @param existingMitigationTemplate Template used to generate the existing mitigation.
     * @param newMitigationName MitigationName of the new mitigation to be created.
     * @param newMitigationTemplate Template to be used for generating the new mitigation.
     * @param newDefinition MitigationDefinition object representing definition of the new mitigation to be created. 
     * @param metrics
     * @return void. Throws an exception if we find a conflicting mitigation
     */
    protected void checkDuplicateDefinition( MitigationDefinition existingDefinition, String existingMitigationName, String existingMitigationTemplate,  
                                            String newMitigationName, String newMitigationTemplate, MitigationDefinition newDefinition, 
                                            TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateAndEditRequestStorageHandler.checkDuplicateDefinition");
        try {
            templateValidator.validateCoexistenceForTemplateAndDevice(newMitigationTemplate, newMitigationName, newDefinition, existingMitigationTemplate, 
                                                                                existingMitigationName, existingDefinition, metrics);
        } finally {
            subMetrics.end();
        }
    }
}
