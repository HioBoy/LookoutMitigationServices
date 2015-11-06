package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateMitigationNameException400;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToFixedActionMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Sets;

/**
 * DDBBasedCreateRequestStorageHandler is responsible for persisting create requests into DDB.
 * As part of persisting this request, based on the existing requests in the table, this handler also determines the workflowId 
 * to be used for the current request.
 */
@ThreadSafe
public class DDBBasedCreateRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedCreateRequestStorageHandler.class);

    // Num Attempts + Retry Sleep Configs.
    public static final int DDB_ACTIVITY_MAX_ATTEMPTS = 10;
    private static final int DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;

    // Prefix tags for logging warnings to be monitored.
    private static final String CREATE_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[CREATE_REQUEST_STORAGE_FAILED]";

    // Keys for TSDMetric property.
    private static final String NUM_ACTIVE_MITIGATIONS_FOR_DEVICE = "NumActiveMitigations";
    private static final String NUM_ATTEMPTS_TO_STORE_CREATE_REQUEST = "NumCreateRequestStoreAttempts";
    
    public static final int INITIAL_MITIGATION_VERSION = 1;

    private final DataConverter jsonDataConverter = new JsonDataConverter();

    private final TemplateBasedRequestValidator templateBasedRequestValidator;
    private final DynamoDB dynamoDB;

    public DDBBasedCreateRequestStorageHandler(AmazonDynamoDBClient dynamoDBClient, String domain, @NonNull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain);

        this.templateBasedRequestValidator = templateBasedRequestValidator;
        this.dynamoDB = new DynamoDB(dynamoDBClient);
    }

    /**
     * Stores the create request into the DDB Table. While storing, it identifies the new workflowId to be associated with this request and returns back the same.
     * The algorithm it uses to identify the workflowId to use is:
     * 1. Identify the deviceNameAndScope that corresponds to this request.
     * 2. Query for all active mitigations for this device.
     * 3. If no active mitigations exist for this device, check with the deviceScope enum and start with the minimum value corresponding to the scope.
     * 4. Else identify the max workflowId for the active mitigations for this device and scope and try to use this maxWorkflowId+1 as the new workflowId
     *    when storing the request.
     * 5. If when storing the request we encounter an exception, it could be either because someone else started using the maxWorkflowId+1 workflowId for that device
     *    or it is some transient exception. In either case, we query the DDB table once again for mitigations >= maxWorkflowId for the device and
     *    continue with step 4. 
     * @param request Request to be stored.
     * @param locations Set of String representing the locations where this request applies.
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@NonNull MitigationModificationRequest request, @NonNull Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);
        
        CreateMitigationRequest createMitigationRequest = (CreateMitigationRequest) request;

        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHandler.storeRequestForWorkflow");
        int numAttempts = 0;
        try {
            String mitigationName = createMitigationRequest.getMitigationName();
            String mitigationTemplate = createMitigationRequest.getMitigationTemplate();

            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);

            String deviceName = deviceNameAndScope.getDeviceName().name();
            String deviceScope = deviceNameAndScope.getDeviceScope().name();

            MitigationDefinition mitigationDefinition = createMitigationRequest.getMitigationDefinition();
            
            // If action is null, check with the MitigationTemplateToFixedActionMapper to get the action for this template.
            // This is required to get the action stored with the mitigation definition in DDB, allowing it to be later displayed on the UI.
            if (mitigationDefinition.getAction() == null) {
                ActionType actionType = MitigationTemplateToFixedActionMapper.getActionTypesForTemplate(mitigationTemplate);
                if (actionType == null) {
                    String msg = "Validation for this request went through successfully, but this request doesn't have any action associated with it and no " +
                                 "mapping found in the MitigationTemplateToFixedActionMapper for the template in this request. Request: " + ReflectionToStringBuilder.toString(createMitigationRequest);
                    LOG.error(msg);
                    throw new RuntimeException(msg);
                }
                mitigationDefinition.setAction(actionType);
            }
            
            Long prevMaxWorkflowId = null;
            Long currMaxWorkflowId = null;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts++ < DDB_ACTIVITY_MAX_ATTEMPTS) {
                prevMaxWorkflowId = currMaxWorkflowId;
                
                // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
                currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, subMetrics);

                // Next, check if the mitigation name has already been used before.
                if (doesMitigationNameExist(deviceName, mitigationName, subMetrics)) {
                    String msg = "MitigationName: " + mitigationName + " already exists for device: " + deviceName;
                    LOG.warn(msg);
                    throw new DuplicateMitigationNameException400(msg);
                }
 
                // Next, check if we have any duplicate mitigations already in place.
                queryAndCheckDuplicateMitigations(deviceName, deviceScope, mitigationDefinition, 
                                                  mitigationName, mitigationTemplate, prevMaxWorkflowId, subMetrics);

                long newWorkflowId = 0;
                // If we didn't get any workflows for the same deviceName and deviceScope, we simply assign the newWorkflowId to be the min for this deviceScope.
                if (currMaxWorkflowId == null) {
                    newWorkflowId = deviceNameAndScope.getDeviceScope().getMinWorkflowId();
                } else {
                    // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                    newWorkflowId = currMaxWorkflowId + 1;
                    sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);
                }

                try {
                    storeRequestInDDB(createMitigationRequest, createMitigationRequest.getMitigationDefinition(),
                           locations, deviceNameAndScope, newWorkflowId, RequestType.CreateRequest, INITIAL_MITIGATION_VERSION, subMetrics);
                    return newWorkflowId;
                } catch (Exception ex) {
                    String msg = "Caught exception when storing create request in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(createMitigationRequest);
                    LOG.warn(msg);
                }
                
                if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    try {
                        Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                    } catch (InterruptedException ignored) {}
                }
            }
            
            // Actual number of attempts is 1 greater than the current value since we increment numAttempts after the check for the loop above.
            numAttempts = numAttempts - 1;

            String msg = CREATE_REQUEST_STORAGE_FAILED_LOG_PREFIX + " - Unable to store create request : " + ReflectionToStringBuilder.toString(createMitigationRequest) +
                         " after " + numAttempts + " attempts";
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_ATTEMPTS_TO_STORE_CREATE_REQUEST, numAttempts);
            subMetrics.end();
        }
    }
    
    private static List<String> NON_FAILED_WORKFLOW_STATUS;
    static {
        NON_FAILED_WORKFLOW_STATUS = Arrays.asList(WorkflowStatus.values()).stream()
                .filter(status -> (status != WorkflowStatus.FAILED) && (status != WorkflowStatus.INDETERMINATE))
                .collect(Collectors.toList());
    }
    
    /**
     * check whether there is a non-failed deployment on this mitigation name, this device name before.
     * @param deviceName : device name
     * @param mitigationName : mitigation name
     * @param metrics : TSD metrics
     * @return true, if there is a successful deployment on this mitigation name, this device name before.
     * false, if there is not.
     */
    boolean doesMitigationNameExist(String deviceName, String mitigationName, TSDMetrics metrics) {
        QuerySpec query = new QuerySpec().withAttributesToGet(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY)
                .withConsistentRead(true).withHashKey(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, deviceName)
                .withRangeKeyCondition(new RangeKeyCondition(
                        DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY).eq(mitigationName))
                .withQueryFilters(new QueryFilter(WORKFLOW_STATUS_KEY).in(NON_FAILED_WORKFLOW_STATUS.toArray()),
                        new QueryFilter(REQUEST_TYPE_KEY).ne(RequestType.DeleteRequest.name()),
                        new QueryFilter(MitigationRequestsModel.DEFUNCT_DATE).notExist())
                        .withMaxResultSize(50);
        
        try(TSDMetrics subMetrics = metrics.newSubMetrics(
                "DDBBasedCreateRequestStorageHandler.doesMitigationNameExist")) {
            try {
                return dynamoDB.getTable(mitigationRequestsTableName).getIndex(MITIGATION_NAME_LSI)
                        .query(query).getTotalCount() != 0;
            } catch (RuntimeException ex) {
                String message = "Failed to query dynamodb for checking doesMitigationNameExist, with query: "
                        + ReflectionToStringBuilder.toString(query);
                LOG.warn(message);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
        }
    }

    /**
     * Query DDB and check if there exists a duplicate request for the create request being processed. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope DeviceScope for the device where the new mitigation needs to be created.
     * @param mitigationDefinition Mitigation definition for the new create request.
     * @param mitigationName Name of the new mitigation being created.
     * @param mitigationTemplate Template being used for the new mitigation being created.
     * @param maxWorkflowIdOnLastAttempt If we had queried this DDB before, we could query for mitigations greaterThanOrEqual to this maxWorkflowId
     *                                   seen from the last attempt, else this value is null and we simply query all active mitigations for this device.
     * @param metrics
     * @return Max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     */
    protected void queryAndCheckDuplicateMitigations(String deviceName, String deviceScope, MitigationDefinition mitigationDefinition, 
                                                     String mitigationName, String mitigationTemplate, Long maxWorkflowIdOnLastAttempt, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHandler.queryAndCheckDuplicateMitigations");
        try {
            Map<String, AttributeValue> lastEvaluatedKey = null;
            
            Set<String> attributesToGet = generateAttributesToGet();
            
            Map<String, Condition> queryFiltersToCheckForDuplicates = createQueryFiltersToCheckForDuplicates();
            
            // Index to use is null if we query by DeviceName + WorkflowId - since we then use the primary key of the table.
            // If we query using DeviceName + UpdateWorkfowId - we set indexToUse to the name of LSI corresponding to these keys.
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
                queryFiltersToCheckForDuplicates.put(UPDATE_WORKFLOW_ID_KEY, condition);
            }
            
            do {
                int numAttempts = 0;
                QueryResult result = null;
                Throwable lastCaughtException = null;
                while (numAttempts++ < DDB_ACTIVITY_MAX_ATTEMPTS) {
                    try {
                        result = getActiveMitigationsForDevice(deviceName, deviceScope, attributesToGet, keyConditions, 
                                                               lastEvaluatedKey, indexToUse, queryFiltersToCheckForDuplicates, subMetrics);
                        lastEvaluatedKey = result.getLastEvaluatedKey();
                        subMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOR_DEVICE, result.getCount());
                        break;
                    } catch (Exception ex) {
                        lastCaughtException = ex;
                        String msg = "Caught exception when querying active mitigations from DDB for device: " + deviceName + " and deviceScope: " + deviceScope + 
                                     ", keyConditions: " + keyConditions + ", indexToUse: " + indexToUse;
                        LOG.warn(msg, ex);
                    }
                    
                    if (numAttempts < DDB_ACTIVITY_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(DDB_ACTIVITY_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
                }
                
                if (numAttempts > DDB_ACTIVITY_MAX_ATTEMPTS) {
                    String msg = CREATE_REQUEST_STORAGE_FAILED_LOG_PREFIX + " - Unable to query active mitigations for device: " + deviceName + " and deviceScope: " + 
                                 deviceScope + ", keyConditions: " + keyConditions + ", indexToUse: " + indexToUse + " after " + numAttempts + " attempts";
                    LOG.warn(msg, lastCaughtException);
                    throw new RuntimeException(msg, lastCaughtException);
                }
                checkForDuplicatesFromDDBResult(deviceName, deviceScope, result, mitigationDefinition, mitigationName, mitigationTemplate, subMetrics);
            } while (lastEvaluatedKey != null);
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Generate a set of attributes to get from the DDB Query.
     * @return Set of string representing the attributes that need to be fetched form the DDB Query.
     */
    private Set<String> generateAttributesToGet() {
        Set<String> attributesToGet = Sets.newHashSet(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_HASH_KEY, 
                                                      DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, 
                                                      DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY, DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY,
                                                      DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY,
                                                      DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY);
        return attributesToGet;
    }
    
    /**
     * Creates the queryFilters to use when querying DDB to check for duplicate mitigation definitions.
     * @return Map<String, Condition> representing the queryFilter to use for the DDB query.
     */
    private Map<String, Condition> createQueryFiltersToCheckForDuplicates() {
        Map<String, Condition> queryFilters = new HashMap<>();
        
        String[] workflowStatuses = WorkflowStatus.values();
        List<AttributeValue> attrValues = new ArrayList<>(workflowStatuses.length - 2);
        for (String workflowStatus : workflowStatuses) {
            if (!workflowStatus.equals(WorkflowStatus.FAILED) && !workflowStatus.equals(WorkflowStatus.INDETERMINATE)) {
                attrValues.add(new AttributeValue(workflowStatus));       
            }
        }
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.IN).withAttributeValueList(attrValues);
        
        queryFilters.put(WORKFLOW_STATUS_KEY, condition);
        
        AttributeValue attrVal = new AttributeValue(RequestType.DeleteRequest.name());
        condition = new Condition().withComparisonOperator(ComparisonOperator.NE);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(REQUEST_TYPE_KEY, condition);
        
        condition = new Condition().withComparisonOperator(ComparisonOperator.NULL);
        queryFilters.put(MitigationRequestsModel.DEFUNCT_DATE, condition);
        
        return queryFilters;
    }

    /**
     * From the QueryResult iterate through the mitigations for this device, check if it is a duplicate of the mitigation requested to be created. If we find a duplicate,
     * we throw back an exception. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope Scope for the device on which the new mitigation is to be created. 
     * @param result QueryResult from the DDB query issued previously to find the existing mitigations for the device where the new mitigation is to be deployed.
     * @param newDefinition MitigationDefinition for the new mitigation that needs to be applied.
     * @param newDefinitionName Name of the new mitigation being created.
     * @param newDefinitionTemplate Template used for the new mitigation being created.
     * @param metrics
     * @return Max workflowId for existing mitigations for the same deviceName and deviceScope. Null if there are no such active mitigations.
     */
    protected void checkForDuplicatesFromDDBResult(String deviceName, String deviceScope, QueryResult result, MitigationDefinition newDefinition, 
                                                   String newDefinitionName, String newDefinitionTemplate, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.checkForDuplicatesFromDDBResult");
        try {
            for (Map<String, AttributeValue> item : result.getItems()) {
                String existingDefinitionAsJSONString = item.get(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY).getS();
                String existingMitigationName = item.get(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY).getS();
                String existingMitigationTemplate = item.get(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY).getS();
                
                // Check if the mitigation name for the existing mitigation and the new mitigation are the same. If so, throw back an exception.
                if (existingMitigationName.equals(newDefinitionName)) {
                    String msg = "MitigationName: " + existingMitigationName + " already exists for device: " + deviceName + ". Existing mitigation template: " + 
                                 existingMitigationTemplate + ". New mitigation template: " + newDefinitionTemplate;
                    LOG.warn(msg);
                    throw new DuplicateRequestException400(msg);
                }

                checkDuplicateDefinition(existingDefinitionAsJSONString, existingMitigationName, existingMitigationTemplate, 
                                         newDefinitionName, newDefinitionTemplate, newDefinition, subMetrics);
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
     * @return void. Throws an exception if we find a duplicate definition. The TemplateBasedValidator is also invoked to check if there is any template
     *               based checks for verifying if the new mitigation can coexist with the new mitigation being requested.
     */
    protected void checkDuplicateDefinition(String existingDefinitionAsJSONString, String existingMitigationName, String existingMitigationTemplate,  
                                            String newMitigationName, String newMitigationTemplate, MitigationDefinition newDefinition, 
                                            TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.checkDuplicateDefinition");
        try {
            MitigationDefinition existingDefinition = getJSONDataConverter().fromData(existingDefinitionAsJSONString, MitigationDefinition.class);
            getTemplateBasedValidator().validateCoexistenceForTemplateAndDevice(newMitigationTemplate, newMitigationName, newDefinition, existingMitigationTemplate, 
                                                                                existingMitigationName, existingDefinition, metrics);
        } finally {
            subMetrics.end();
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
