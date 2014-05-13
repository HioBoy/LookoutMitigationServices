package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
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

    public DDBBasedCreateRequestStorageHandler(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain, @Nonnull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain);

        Validate.notNull(templateBasedRequestValidator);
        this.templateBasedRequestValidator = templateBasedRequestValidator;
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
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@Nonnull MitigationModificationRequest request, @Nonnull TSDMetrics metrics) {
        Validate.notNull(request);
        Validate.notNull(metrics);
        
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
            String mitigationDefinitionAsJSONString = getJSONDataConverter().toData(mitigationDefinition);
            int mitigationDefinitionHash = mitigationDefinitionAsJSONString.hashCode();

            Long prevMaxWorkflowId = null;
            Long currMaxWorkflowId = null;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts++ < DDB_ACTIVITY_MAX_ATTEMPTS) {
            	prevMaxWorkflowId = currMaxWorkflowId;
            	
                // First, retrieve the current maxWorkflowId for the mitigations for the same device+scope.
                currMaxWorkflowId = getMaxWorkflowIdForDevice(deviceName, deviceScope, prevMaxWorkflowId, subMetrics);
                
                // Next, check if we have any duplicate mitigations already in place.
                queryAndCheckDuplicateMitigations(deviceName, deviceScope, mitigationDefinition, mitigationDefinitionHash, 
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
                    storeRequestInDDB(createMitigationRequest, deviceNameAndScope, newWorkflowId, RequestType.CreateRequest, INITIAL_MITIGATION_VERSION, subMetrics);
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
    
    /**
     * Query DDB and check if there exists a duplicate request for the create request being processed. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope DeviceScope for the device where the new mitigation needs to be created.
     * @param mitigationDefinition Mitigation definition for the new create request.
     * @param mitigationDefinitionHash Hashcode of json representation of the mitigation definition in the new create request.
     * @param mitigationName Name of the new mitigation being created.
     * @param mitigationTemplate Template being used for the new mitigation being created.
     * @param maxWorkflowIdOnLastAttempt If we had queried this DDB before, we could query for mitigations greaterThanOrEqual to this maxWorkflowId
     *                                   seen from the last attempt, else this value is null and we simply query all active mitigations for this device.
     * @param metrics
     * @return Max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     */
    protected void queryAndCheckDuplicateMitigations(String deviceName, String deviceScope, MitigationDefinition mitigationDefinition, int mitigationDefinitionHash, 
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
                checkForDuplicatesFromDDBResult(deviceName, deviceScope, result, mitigationDefinition, mitigationDefinitionHash, mitigationName, mitigationTemplate, subMetrics);
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
        
        AttributeValue attrVal = new AttributeValue(WorkflowStatus.FAILED);
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.NE);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(WORKFLOW_STATUS_KEY, condition);
        
        attrVal = new AttributeValue(RequestType.DeleteRequest.name());
        condition = new Condition().withComparisonOperator(ComparisonOperator.NE);
        condition.setAttributeValueList(Arrays.asList(attrVal));
        queryFilters.put(REQUEST_TYPE_KEY, condition);
        
        return queryFilters;
    }

    /**
     * From the QueryResult iterate through the mitigations for this device, check if it is a duplicate of the mitigation requested to be created. If we find a duplicate,
     * we throw back an exception. Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope Scope for the device on which the new mitigation is to be created. 
     * @param result QueryResult from the DDB query issued previously to find the existing mitigations for the device where the new mitigation is to be deployed.
     * @param newDefinition MitigationDefinition for the new mitigation that needs to be applied.
     * @param newDefinitionHashCode Hashcode for the MitigationDefinition of the new mitigation that needs to be applied.
     * @param newDefinitionName Name of the new mitigation being created.
     * @param newDefinitionTemplate Template used for the new mitigation being created.
     * @param metrics
     * @return Max workflowId for existing mitigations for the same deviceName and deviceScope. Null if there are no such active mitigations.
     */
    protected void checkForDuplicatesFromDDBResult(String deviceName, String deviceScope, QueryResult result, MitigationDefinition newDefinition, 
                                                   int newDefinitionHashCode, String newDefinitionName, String newDefinitionTemplate, TSDMetrics metrics) {
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
                                         newDefinitionName, newDefinitionTemplate, newDefinition, newDefinitionHashCode, subMetrics);
            }
        } finally {
            subMetrics.end();
        }
    }

    /**
     * We currently check if 2 definitions are exactly identical. There could be cases where 2 definitions are equivalent, but not identical (eg:
     * when they have the same constraints, but the constraints are in different order) - in those cases we don't treat them as identical for now.
     * We could do so, by enforcing a particular ordering to the mitigation definitions when we persist the definition - however it might get tricky
     * to do so for different use-cases, eg: for IPTables maybe the user crafted rules such that they are in a certain order for a specific reason.
     * We could have a deeper-check based on the template - which checks if 2 mitigations are equivalent, but we don't have a strong use-case for such as of now, 
     * hence keeping the comparison simple for now.
     * 
     * We also first check if the hashcode for json representation of the definition matches - which acts as a shortcut to avoid deep inspection of the definitions
     * for non-identical strings.
     *  
     * Protected for unit-testing.
     * @param existingDefinitionAsJSONString MitigationDefinition for an existing mitigation represented as a JSON string.
     * @param existingMitigationName MitigationName for the existing mitigation.
     * @param existingMitigationTemplate Template used to generate the existing mitigation.
     * @param newMitigationName MitigationName of the new mitigation to be created.
     * @param newMitigationTemplate Template to be used for generating the new mitigation.
     * @param newDefinition MitigationDefinition object representing definition of the new mitigation to be created. 
     * @param newDefinitionHash Hashcode of the JSON representation of the new mitigation definition.  
     * @param metrics
     * @return void. Throws an exception if we find a duplicate definition. The TemplateBasedValidator is also invoked to check if there is any template
     *               based checks for verifying if the new mitigation can coexist with the new mitigation being requested.
     */
    protected void checkDuplicateDefinition(String existingDefinitionAsJSONString, String existingMitigationName, String existingMitigationTemplate,  
                                            String newMitigationName, String newMitigationTemplate, MitigationDefinition newDefinition, 
                                            int newDefinitionHash, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.checkDuplicateDefinition");
        try {
            int existingMitigationHash = existingDefinitionAsJSONString.hashCode();

            MitigationDefinition existingDefinition = getJSONDataConverter().fromData(existingDefinitionAsJSONString, MitigationDefinition.class);
            if ((existingMitigationHash == newDefinitionHash) && newDefinition.equals(existingDefinition)) {
                String msg = "Found identical mitigation definition: " + existingMitigationName + " for existingTemplate: " + existingMitigationTemplate +
                             " with definition: " + existingDefinitionAsJSONString + " for request with MitigationName: " + newMitigationName + 
                             " and MitigationTemplate: " + newMitigationTemplate;
                LOG.info(msg);
                throw new DuplicateDefinitionException400(msg);
            }
            getTemplateBasedValidator().validateCoexistenceForTemplateAndDevice(newMitigationTemplate, newMitigationName, newDefinition, existingMitigationTemplate, 
                                                                                existingMitigationName, existingDefinition);
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
