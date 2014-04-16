package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.constants.RequestType;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
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
    private static final int NEW_WORKFLOW_ID_MAX_ATTEMPTS = 10;
    private static final int NEW_WORKFLOW_ID_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;

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
     * @param createMitigationRequest Request to be stored.
     * @param metrics
     * @return The workflowId that this request was stored with, using the algorithm above.
     */
    @Override
    public long storeRequestForWorkflow(@Nonnull MitigationModificationRequest createMitigationRequest, @Nonnull TSDMetrics metrics) {
        Validate.notNull(createMitigationRequest);
        Validate.notNull(metrics);

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

            Long maxWorkflowId = null;
            
            // Get the max workflowId for existing mitigations, increment it by 1 and store it in the DDB. Return back the new workflowId
            // if successful, else end the loop and throw back an exception.
            while (numAttempts++ < NEW_WORKFLOW_ID_MAX_ATTEMPTS) {
                // On the first attempt, the maxWorkflowId is null, so we would simply query for all active mitigations for the device.
                // On subsequent attempts, we would constraint our query only to the workflowIds >= maxWorkflowId.
                maxWorkflowId = getMaxWorkflowIdFromDDBTable(deviceName, deviceScope, mitigationDefinition, mitigationDefinitionHash, 
                                                             mitigationName, mitigationTemplate, maxWorkflowId, subMetrics);

                long newWorkflowId = 0;
                // If we didn't get any workflows for the same deviceName and deviceScope, we simply assign the newWorkflowId to be the min for this deviceScope.
                if (maxWorkflowId == null) {
                    newWorkflowId = deviceNameAndScope.getDeviceScope().getMinWorkflowId();
                } else {
                    // Increment the maxWorkflowId to use as the newWorkflowId and sanity check to ensure the new workflowId is still within the expected range.
                    newWorkflowId = ++maxWorkflowId;
                    sanityCheckWorkflowId(newWorkflowId, deviceNameAndScope);
                }

                try {
                    storeRequestInDDB(createMitigationRequest, deviceNameAndScope, newWorkflowId, RequestType.CreateRequest.name(), INITIAL_MITIGATION_VERSION, subMetrics);
                    return newWorkflowId;
                } catch (Exception ex) {
                    String msg = "Caught exception when storing create request in DDB with newWorkflowId: " + newWorkflowId + " for DeviceName: " + deviceName + 
                                 " and deviceScope: " + deviceScope + " AttemptNum: " + numAttempts + ". For request: " + ReflectionToStringBuilder.toString(createMitigationRequest);
                    LOG.warn(msg);

                    if (numAttempts < NEW_WORKFLOW_ID_MAX_ATTEMPTS) {
                        try {
                            Thread.sleep(NEW_WORKFLOW_ID_RETRY_SLEEP_MILLIS_MULTIPLIER * numAttempts);
                        } catch (InterruptedException ignored) {}
                    }
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
     * Check if the new workflowId we're going to use is within the valid range for this deviceScope.
     * @param workflowId
     * @param deviceNameAndScope
     */
    private void sanityCheckWorkflowId(long workflowId, DeviceNameAndScope deviceNameAndScope) {
        DeviceScope deviceScope = deviceNameAndScope.getDeviceScope();
        if ((workflowId < deviceScope.getMinWorkflowId()) || (workflowId > deviceScope.getMaxWorkflowId())) {
            String msg = "Received workflowId = " + workflowId + " which is out of the valid range for the device scope: " + deviceScope.name() +
                         " expectedMin: " + deviceScope.getMinWorkflowId() + " expectedMax: " + deviceScope.getMaxWorkflowId();
            LOG.fatal(msg);
            throw new InternalServerError500(msg);
        }
    }

    /**
     * Get the max workflowId for all the active mitigations currently in place for this device.
     * Protected for unit-testing.
     * @param deviceName DeviceName for which the new mitigation needs to be created.
     * @param deviceScope DeviceScope for the device where the new mitigation needs to be created.
     * @param mitigationDefinition Definition of the new mitigation being created.
     * @param mitigationDefinitionHash Hashcode of the new mitigation definition being created.
     * @param mitigationName Name of the new mitigation being created.
     * @param mitigationTemplate Template being used for the new mitigation being created.
     * @param maxWorkflowIdOnLastAttempt If we had queried this DDB before, we could query for mitigations greaterThanOrEqual to this maxWorkflowId
     *                                   seen from the last attempt, else this value is null and we simply query all active mitigations for this device.
     * @param metrics
     * @return Max WorkflowId for existing mitigations. Null if no mitigations exist for this deviceName and deviceScope.
     */
    protected Long getMaxWorkflowIdFromDDBTable(String deviceName, String deviceScope, MitigationDefinition mitigationDefinition, int mitigationDefinitionHash, 
                                                String mitigationName, String mitigationTemplate, Long maxWorkflowIdOnLastAttempt, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHandler.getMaxWorkflowIdFromDDBTable");
        Long maxWorkflowId = null;
        try {
            Set<String> attributesToGet = generateAttributesToGet();
            
            // Index to use is null if we query by DeviceName + WorkflowId - since we then use the primary key of the table.
            // If we query using DeviceName + UpdateWorkfowId - we set indexToUse to the name of LSI corresponding to these keys.
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
                maxWorkflowId = checkDuplicatesAndGetMaxWorkflowId(deviceName, deviceScope, result, mitigationDefinition, mitigationDefinitionHash, 
                                                                   mitigationName, mitigationTemplate, subMetrics);
            } else {
                return maxWorkflowId;
            }

            lastEvaluatedKey = result.getLastEvaluatedKey();
            while (lastEvaluatedKey != null) {
                result = getActiveMitigationsForDevice(deviceName, attributesToGet, keyConditions, lastEvaluatedKey, indexToUse, subMetrics);
                subMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOR_DEVICE, result.getCount());

                if (result.getCount() > 0) {
                    maxWorkflowId = checkDuplicatesAndGetMaxWorkflowId(deviceName, deviceScope, result, mitigationDefinition, mitigationDefinitionHash, 
                                                                       mitigationName, mitigationTemplate, subMetrics);
                }
                lastEvaluatedKey = result.getLastEvaluatedKey();
            }
            return maxWorkflowId;
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
                                                      DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY);
        return attributesToGet;
    }


    /**
     * From the QueryResult iterate through the mitigations for this device, skip the inactive one and ones that don't match the 
     * deviceScope. For the active ones, check if it is a duplicate of the mitigation requested to be created. If we find a duplicate,
     * we throw back an exception, else we track the workflowId of the existing mitigation - so at the end of the iteration, if we haven't
     * found any duplicate mitigations, we return back the max of the workflowIds for the currently deployed mitigations.
     * Protected for unit-testing.
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
    protected Long checkDuplicatesAndGetMaxWorkflowId(String deviceName, String deviceScope, QueryResult result, MitigationDefinition newDefinition, 
                                                      int newDefinitionHashCode, String newDefinitionName, String newDefinitionTemplate, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHelper.checkDuplicatesAndGetMaxWorkflowId");
        try {
            Long maxWorkflowId = null;
            for (Map<String, AttributeValue> item : result.getItems()) {
                // Check if this existing mitigation workflow was for the same scope as the new request.
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
                
                // If this existing mitigation workflow failed to complete, then we don't bother checking it for being duplicate with the new request.
                String existingWorkflowStatus = item.get(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY).getS();
                if (existingWorkflowStatus.equals(WorkflowStatus.FAILED)) {
                    continue;
                }

                String existingDefinitionAsJSONString = item.get(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY).getS();
                String existingMitigationName = item.get(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY).getS();
                String existingMitigationTemplate = item.get(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY).getS();

                checkDuplicateDefinition(existingDefinitionAsJSONString, existingMitigationName, existingMitigationTemplate, 
                                         newDefinitionName, newDefinitionTemplate, newDefinition, newDefinitionHashCode, subMetrics);
            }
            return maxWorkflowId;
        } finally {
            subMetrics.end();
        }
    }

    /**
     * Generate the keys for querying active mitigations for the device passed as input. Protected for unit-testing.
     * An active mitigation is one whose UpdateWorkflowId column hasn't been updated in the MitigationRequests table.
     * @param deviceName Device for whom we need to find the active mitigations.
     * @return Keys for querying active mitigations for the device passed as input.
     */
    protected Map<String, Condition> getKeysForActiveMitigationsForDevice(String deviceName) {
        Map<String, Condition> keyConditions = new HashMap<>();
        
        Set<AttributeValue> keyValues = new HashSet<>();
        AttributeValue keyValue = new AttributeValue(deviceName);
        keyValues.add(keyValue);

        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);

        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, condition);

        keyValues = new HashSet<>();
        keyValue = new AttributeValue();
        keyValue.setN(String.valueOf(UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS));
        keyValues.add(keyValue);

        condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY, condition);

        return keyConditions;
    }

    /**
     * Generate the keys for querying mitigations for the device passed as input and whose workflowIds are equal or greater than the workflowId passed as input.
     * Protected for unit-testing
     * @param deviceName Device for whom we need to find the active mitigations.
     * @param workflowId WorkflowId which we need to constraint our query by. We should be querying for existing mitigations whose workflowIds are >= this value.
     * @return Keys for querying mitigations for the device passed as input with workflowIds >= the workflowId passed as input above.
     */
    protected Map<String, Condition> getKeysForDeviceAndWorkflowId(String deviceName, Long workflowId) {
        Map<String, Condition> keyConditions = new HashMap<>();

        Set<AttributeValue> keyValues = new HashSet<>();
        AttributeValue keyValue = new AttributeValue(deviceName);
        keyValues.add(keyValue);

        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);

        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, condition);

        keyValues = new HashSet<>();
        keyValue = new AttributeValue();
        keyValue.setN(String.valueOf(workflowId));
        keyValues.add(keyValue);

        condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.GE);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, condition);

        return keyConditions;
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
                                                                                  existingMitigationName, existingDefinition, subMetrics);
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
