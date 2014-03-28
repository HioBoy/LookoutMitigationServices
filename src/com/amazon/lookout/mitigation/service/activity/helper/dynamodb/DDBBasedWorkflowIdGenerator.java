package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.dynamo.DynamoDBHelper;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.WorkflowIdGenerator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

/**
 * This class is responsible for helping generate new workflowIds using the MITIGATION_DEVICE_WORKFLOW DynamoDB table.
 * When requested to create a new workflowId, this generator follows the steps below:
 * 1. Identify the device name and scope corresponding to the mitigation template being used in the request.
 * 2. If the row corresponding to the DeviceName and DeviceScope is currently unlocked, then lock it and increment the workflowId.
 * 3.1 If the row corresponding to the DeviceName and DeviceScope is currently locked, then retry for a fixed number of times.
 * 3.2 If after all retries, the row is still locked - check if we were blocked on the same lock across all attempts.
 * 3.3 If that is the case, the process holding this lock is either no longer running or is taking way longer than expected, hence
 *     in this case we try to forcefully acquire the lock.
 * 3.4 However, if we have been blocked on different items across all our retries, then it implies there is an unusually heavy number
 *     of requests for this device. In this case we fail the request.
 * 4. If we have the new workflowId, we return that back to the client.
 * 
 * More details of this logic can be found here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#Data_Model_Usage
 * 
 * This generator works on the DynamoDBTable prefixed by: MITIGATION_DEVICE_WORKFLOW
 * Details of this table can be found here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#MITIGATION_DEVICE_WORKFLOW
 * 
 * Note: Once the client calls generateWorkflowId - it is the responsibility of the client to call this generator to confirm acquiring this new
 *       workflowId, so the generator can close all the appropriate locks.
 *
 */
@ThreadSafe
public class DDBBasedWorkflowIdGenerator implements WorkflowIdGenerator {
    
    private static final Log LOG = LogFactory.getLog(DDBBasedWorkflowIdGenerator.class);
    
    public static final String WORKFLOW_ID_GENERATION_TABLE_PREFIX = "MITIGATION_DEVICE_WORKFLOW_";
    public static final String DEVICE_NAME_KEY = "DeviceName";
    public static final String DEVICE_SCOPE_KEY = "DeviceScope";
    public static final String WORKFLOW_ID_KEY = "WorkflowId";
    public static final String STATUS_KEY = "Status";
    
    public static final int WORKFLOW_ID_INCREMENT_STEP = 1;
    
    public static final int DDB_UPDATE_MAX_RETRIES = 10;
    private static final int DDB_UPDATE_RETRY_SLEEP_MILLIS_MULTIPLIER = 100;
    
    private static final String NUM_DDB_UPDATES_WHEN_UNLOCKING = "NumUnlockUpdateAttempts";
    private static final String NUM_DDB_UPDATES_WHEN_LOCKING = "NumLockUpdateAttempts";
    
    private static final String WORKFLOW_ID_GENERATION_FAILURE_LOG_KEY = "[WORKFLOW_ID_GENERATION_FAILED]";
    private static final String WORKFLOW_ID_GENERATION_UNLOCKING_FAILURE_LOG_KEY = "[WORKFLOW_ID_GENERATION_UNLOCKING_FAILED]";
    
    private final AmazonDynamoDBClient dynamoDBClient;
    private final String workflowIdGenerationTableName;
    
    private enum DDBRowLockStatus {
        LOCKED,
        UNLOCKED
    };
    
    public DDBBasedWorkflowIdGenerator(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain) {
        Validate.notNull(dynamoDBClient);
        this.dynamoDBClient = dynamoDBClient;
        
        // We have a separate table per domain (beta,gamma,prod) - hence we compute the appropriate table name using value of domain here.
        // We do an uppercase for the passed domain to not rely on the caller to pass it in the right case since this string might be used
        // for other purposes such as metrics.
        Validate.notEmpty(domain);
        this.workflowIdGenerationTableName = WORKFLOW_ID_GENERATION_TABLE_PREFIX + domain.toUpperCase();
    }

    /**
     * Generate a new workflowId using the steps mentioned here: https://w.amazon.com/index.php/Lookout/Design/LookoutMitigationService/Details#Data_Model_Usage
     * @param mitigationTemplate Mitigation template used in the request.
     * @param metrics
     * @return workflowId generated using the DynamoDBTable. We throw an InternalServerError500 if we couldn't find the mapping since we expect the caller to have 
     *                    validated the request and hence the template for its validity before calling this generator.
     */
    @Override
    public long generateWorkflowId(@Nonnull String mitigationTemplate, @Nonnull TSDMetrics metrics) {
        Validate.notNull(mitigationTemplate);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.generateWorkflowId");
        try {
            LOG.debug("Requesting a new workflowId for mitigationTemplate: " + mitigationTemplate);
            
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            if (deviceNameAndScope == null) {
                String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY + ": No DeviceNameAndScope mapping found for template: " + mitigationTemplate;
                LOG.warn(msg);
                throw new InternalServerError500(msg);
            }
            
            Map<String, AttributeValueUpdate> attributeValueUpdates = generateAttributeUpdatesForLocking();
            Map<String, AttributeValue> keyValues = generateKeyForUpdate(deviceNameAndScope);
            Map<String, ExpectedAttributeValue> expectedAttributeValues = generateExpectedAttributeValuesForLocking();
            
            return getWorkflowIdFromDDB(attributeValueUpdates, keyValues, expectedAttributeValues, subMetrics);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Once the workflowId has been generated and the value was returned back to the caller for processing, the caller
     * calls this method to confirm acquiring the new workflowId, to allow this generator to release any held locks.
     * @param mitigationTemplate MitigationTemplate used in the request.
     * @oaram workflowId The new workflowId that is being confirmed.
     * @param metrics
     * @return void. We don't return anything from this method. But we do throw an exception if we couldn't unlock the row in DDB. 
     *               We also throw an InternalServerError500 if we couldn't find the mapping since we expect the caller to have 
     *               validated the request and hence the template for its validity before calling this generator.
     */
    @Override
    public void confirmAcquiringWorkflowId(@Nonnull String mitigationTemplate, long workflowId, @Nonnull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);
        Validate.isTrue(workflowId >= 0);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.confirmAcquiringWorkflowId");
        int attemptNum = 0;
        try {
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            if (deviceNameAndScope == null) {
                String msg = MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY + ": No DeviceNameAndScope mapping found for template: " + mitigationTemplate;
                LOG.warn(msg);
                throw new InternalServerError500(msg);
            }
            
            Map<String, AttributeValueUpdate> attributeUpdates = generateAttributeUpdatesForUnlocking();
            Map<String, AttributeValue> keyValues = generateKeyForUpdate(deviceNameAndScope);
            Map<String, ExpectedAttributeValue> expectedValues = generateExpectedAttributeValuesForUnlocking(workflowId);
            
            while (attemptNum++ < DDB_UPDATE_MAX_RETRIES) {
                try {
                    updateItemInDDB(attributeUpdates, keyValues, expectedValues, subMetrics);
                    return;
                } catch (ConditionalCheckFailedException ex) {
                    String msg = WORKFLOW_ID_GENERATION_UNLOCKING_FAILURE_LOG_KEY + " - Lock held to generate workflowId: " + workflowId + " for mitigationTemplate: " + 
                                 mitigationTemplate + " was forcibly revoked. " + "AttributeUpdates: " + attributeUpdates + " KeyValues: " + keyValues + 
                                 " ExpectedValues: " + expectedValues + " TotNumAttemptsN: " + attemptNum;
                    LOG.warn(msg, ex);
                    throw new RuntimeException(msg, ex);
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to release lock for workflowId: " + workflowId + " for mitigationTemplate: " + mitigationTemplate + 
                                 ". AttributeUpdates: " + attributeUpdates + " KeyValues: " + keyValues + " ExpectedValues: " + expectedValues + " TotNumAttemptsN: " + attemptNum;
                    LOG.warn(msg, ex);
                    
                    if (attemptNum < DDB_UPDATE_MAX_RETRIES) {
                        try {
                            Thread.sleep((long) (getUpdateRetrySleepMillis() * attemptNum));
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            String msg = WORKFLOW_ID_GENERATION_UNLOCKING_FAILURE_LOG_KEY + " - Lock held to generate workflowId: " + workflowId + " for mitigationTemplate: " + 
                         mitigationTemplate + " was forcibly revoked. " + "AttributeUpdates: " + attributeUpdates + " KeyValues: " + keyValues + 
                         " ExpectedValues: " + expectedValues + " TotNumAttemptsN: " + attemptNum;
            LOG.warn(msg);
            throw new RuntimeException(msg);
        } finally {
            subMetrics.addCount(NUM_DDB_UPDATES_WHEN_UNLOCKING, attemptNum);
            subMetrics.end();
        }
    }
    
    /**
     * Performs the update of the appropriate row in the dynamoDBTable to increment the workflowId to be used as the new workflowId.
     * When updating, we request the updated values after the update, allowing us to access the incremented workflowId.
     * If we get a ConditionalCheckFailedException we keep a track of whether we've been blocked on the same lock, if so, we
     * forcefully revoke the lock, else we give up and throw back an exception.
     * @param attributeUpdates AttributeValues to update.
     * @param keyValues Key corresponding to the row that needs the update.
     * @param expectedValues Values we expect for certain fields in the row before we execute the update. 
     * @param metrics
     * @return WorkflowId - the incremented workflowId based on the row update.
     */
    private long getWorkflowIdFromDDB(Map<String, AttributeValueUpdate> attributeUpdates, Map<String, AttributeValue> keyValues, 
                                      Map<String, ExpectedAttributeValue> expectedValues, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.getWorkflowIdFromDDB");
        int attemptNum = 0;
        try {
            Map<String, AttributeValue> blockingItem = null;
            
            while (attemptNum++ <= DDB_UPDATE_MAX_RETRIES) {
                try {
                    UpdateItemResult result = updateItemInDDB(attributeUpdates, keyValues, expectedValues, subMetrics);
                    return getWorkflowIdFromUpdatedItem(result);
                } catch (Exception ex) {
                    String msg = "Caught Exception when trying to get a new workflowId using ExpectedValues: " + expectedValues + " and Key: " + 
                                 keyValues + " and attributeUpdates: " + attributeUpdates + ". AttemptNum: " + attemptNum;
                    LOG.warn(msg, ex);
                    System.out.println("Exception: " + msg + " : " + ex.getMessage());
                    
                    if ((blockingItem == null) && (ex instanceof ConditionalCheckFailedException)) {
                        try {
                            blockingItem = getItemBlockingUpdate(keyValues, subMetrics);
                        } catch (Exception getItemException) {
                            LOG.warn("Caught exception when calling getItemAttributes from DDB table for key: " + keyValues + " in table: " + workflowIdGenerationTableName, getItemException);
                        }
                    }
                    
                    if (attemptNum < DDB_UPDATE_MAX_RETRIES) {
                        try {
                            long sleepMillis = getUpdateRetrySleepMillis() * attemptNum;
                            System.out.println("Sleeping for: " + sleepMillis + " millis. AttemptNum: " + attemptNum + " Millis: " + getUpdateRetrySleepMillis());
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            // Total number of attempts is 1 less than the current value for attemptNum.
            int totNumAttempts = attemptNum - 1;
            
            if ((blockingItem != null) && shouldForceUpdate(blockingItem, keyValues, subMetrics)) {
                expectedValues = generateExpectedAttributeValuesForForceUpdate(blockingItem);
                try {
                    UpdateItemResult result = forceUpdateWorkflowIdInDDB(attributeUpdates, keyValues, expectedValues, subMetrics);
                    return getWorkflowIdFromUpdatedItem(result);
                } catch (Exception ex) {
                    String msg = WORKFLOW_ID_GENERATION_FAILURE_LOG_KEY + ": Unable to update workflowIdTable to get a new workflowId after: " + totNumAttempts + " number of attempts. " +
                            "TableName: " + workflowIdGenerationTableName + " AttributesToUpdate: " + attributeUpdates + " for key: " + keyValues + " BlockingItem: " + 
                            blockingItem + ", ExpectedValues for blocking item: " + expectedValues;
                    LOG.warn(msg, ex);
                    throw new RuntimeException(msg);
                }
            } else {
                String msg = WORKFLOW_ID_GENERATION_FAILURE_LOG_KEY + ": Unable to update workflowIdTable to get a new workflowId after: " + totNumAttempts + " number of attempts. " +
                        "TableName: " + workflowIdGenerationTableName + " AttributesToUpdate: " + attributeUpdates + " for key: " + keyValues + " BlockingItem: " + 
                        blockingItem + ", ExpectedValues for blocking item: " + expectedValues;
           
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }
        } finally {
            subMetrics.addCount(NUM_DDB_UPDATES_WHEN_LOCKING, attemptNum);
            subMetrics.end();
        }
    }
    
    /**
     * Determines if we should forcefully revoke the previously held lock to go ahead with the update.
     * We would do so only if we have been blocked on the same item all the while.
     * @param firstBlockingItem Item we were blocked on the first time we noticed a ConditionalCheckFailedException.
     * @param keyValues Key of the Item we were attempting to update.
     * @param metrics
     * @return True if we should forcefully revoke the lock, in case the lock was held by the same item through all our retries. False otherwise.
     */
    private boolean shouldForceUpdate(Map<String, AttributeValue> firstBlockingItem, Map<String, AttributeValue> keyValues, TSDMetrics metrics) {
        try {
            Map<String, AttributeValue> currentlyBlockingItem =  getItemBlockingUpdate(keyValues, metrics);
            if (firstBlockingItem.equals(currentlyBlockingItem)) {
                return true;
            }
        } catch (Exception ex) {
            LOG.warn("Caught exception when calling getItemAttributes from DDB table for key: " + keyValues + " in table: " + workflowIdGenerationTableName, ex);
        }
        return false;
    }
    
    /**
     * Protected to allow for unit-tests. Simply delegates update to the DynamoDBHelper.
     * @param attributeUpdates AttributeValues to update.
     * @param keyValues Key corresponding to the row that needs the update.
     * @param expectedValues Values we expect for certain fields in the row before we execute the update.
     * @param metrics
     * @return UpdateItemResult - Representing the new values that got updated.
     */
    protected UpdateItemResult updateItemInDDB(Map<String, AttributeValueUpdate> attributeUpdates, Map<String, AttributeValue> keyValues, 
                                               Map<String, ExpectedAttributeValue> expectedValues, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.updateItemInDDB");
        try {
            return DynamoDBHelper.updateItem(dynamoDBClient, workflowIdGenerationTableName, attributeUpdates, keyValues, 
                                             expectedValues, null, null, ReturnValue.UPDATED_NEW);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Protected to allow for unit-tests. Simply delegates update to the DynamoDBHelper.
     * Note that this method is identical to the updateItemInDDB method defined above, but having this separate method makes unit-testing much easier 
     * to validate the number of times we attempt to force update and hence the decision.
     * @param attributeUpdates AttributeValues to update.
     * @param keyValues Key corresponding to the row that needs the update.
     * @param expectedValues Values we expect for certain fields in the row before we execute the update.
     * @param metrics
     * @return UpdateItemResult - Representing the new values that got updated.
     */
    protected UpdateItemResult forceUpdateWorkflowIdInDDB(Map<String, AttributeValueUpdate> attributeUpdates, Map<String, AttributeValue> keyValues, 
                                                          Map<String, ExpectedAttributeValue> expectedValues, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.forceUpdateWorkflowIdInDDB");
        try {
            return DynamoDBHelper.updateItem(dynamoDBClient, workflowIdGenerationTableName, attributeUpdates, keyValues, 
                                              expectedValues, null, null, ReturnValue.UPDATED_NEW);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * @return Map of attribute name to the AttributeValue it needs to be updated to, for locking the row.
     */
    private Map<String, AttributeValueUpdate> generateAttributeUpdatesForLocking() {
        Map<String, AttributeValueUpdate> attributeValueUpdates = new HashMap<>();
        
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setValue(new AttributeValue(DDBRowLockStatus.LOCKED.name()));
        attributeValueUpdates.put(STATUS_KEY, update);
        
        AttributeValue value = new AttributeValue();
        value.setN(String.valueOf(WORKFLOW_ID_INCREMENT_STEP));
        update = new AttributeValueUpdate();
        update.setAction(AttributeAction.ADD);
        update.setValue(value);
        attributeValueUpdates.put(WORKFLOW_ID_KEY, update);
        
        return attributeValueUpdates;
    }
    
    /**
     * @return Map of attribute name to the AttributeValue it needs to be updated to, for unlocking the locked row.
     */
    private Map<String, AttributeValueUpdate> generateAttributeUpdatesForUnlocking() {
        Map<String, AttributeValueUpdate> attributeValueUpdates = new HashMap<>();
        
        AttributeValueUpdate update = new AttributeValueUpdate();
        update.setValue(new AttributeValue(DDBRowLockStatus.UNLOCKED.name()));
        attributeValueUpdates.put(STATUS_KEY, update);
        
        return attributeValueUpdates;
    }
    
    /**
     * Generate key corresponding to the row that needs to be updated. The key is identified by the deviceName and deviceScope.
     * @param deviceNameAndScope
     * @return Map of attribute name and its AttributeValue representing the key corresponding to the row that needs to be updated.
     */
    private Map<String, AttributeValue> generateKeyForUpdate(DeviceNameAndScope deviceNameAndScope) {
        Map<String, AttributeValue> keyValues = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(deviceNameAndScope.getDeviceName().name());
        keyValues.put(DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(deviceNameAndScope.getDeviceScope().name());
        keyValues.put(DEVICE_SCOPE_KEY, attributeValue);
        
        return keyValues;
    }
    
    /**
     * @return - Create a Map of attribute name and the ExpectedAttributeValue we expect it to be set to, for locking the row.
     */
    private Map<String, ExpectedAttributeValue> generateExpectedAttributeValuesForLocking() {
        Map<String, ExpectedAttributeValue> expectedAttributeValues = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(DDBRowLockStatus.UNLOCKED.name());
        
        ExpectedAttributeValue expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(STATUS_KEY, expectedAttributeValue);
        
        return expectedAttributeValues;
    }
    
    /**
     * @param workflowId WorkflowId used to set expectation when unlocking a previously locked row.
     * @return - Create a Map of attribute name and the ExpectedAttributeValue we expect it to be set to, for locking the row.
     */
    private Map<String, ExpectedAttributeValue> generateExpectedAttributeValuesForUnlocking(long workflowId) {
        Map<String, ExpectedAttributeValue> expectedAttributeValues = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(DDBRowLockStatus.LOCKED.name());
        ExpectedAttributeValue expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(STATUS_KEY, expectedAttributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(workflowId));
        expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(WORKFLOW_ID_KEY, expectedAttributeValue);
        
        return expectedAttributeValues;
    }
    
    /**
     * @param blockingItemToUpdate Item whose lock needs to be forcefully revoked.
     * @return - Create a Map of attribute name and the ExpectedAttributeValue we expect it to be set to, for forcefully locking the row.
     */
    private Map<String, ExpectedAttributeValue> generateExpectedAttributeValuesForForceUpdate(Map<String, AttributeValue> blockingItemToUpdate) {
        Map<String, ExpectedAttributeValue> expectedAttributeValues = new HashMap<>();
        
        AttributeValue attributeValue = blockingItemToUpdate.get(STATUS_KEY);
        ExpectedAttributeValue expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(STATUS_KEY, expectedAttributeValue);
        
        attributeValue = blockingItemToUpdate.get(WORKFLOW_ID_KEY);
        expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(WORKFLOW_ID_KEY, expectedAttributeValue);
        
        attributeValue = blockingItemToUpdate.get(DEVICE_NAME_KEY);
        expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(DEVICE_NAME_KEY, expectedAttributeValue);
        
        attributeValue = blockingItemToUpdate.get(DEVICE_SCOPE_KEY);
        expectedAttributeValue = new ExpectedAttributeValue();
        expectedAttributeValue.setExists(true);
        expectedAttributeValue.setValue(attributeValue);
        expectedAttributeValues.put(DEVICE_SCOPE_KEY, expectedAttributeValue);
        
        return expectedAttributeValues;
    }
    
    /**
     * Queries DDB to get the item blocking the current request from locking the row. Protected for unit-tests.
     * @param keyValues Key representing the row that needs to be locked. 
     * @return returns Map of attribute name and its AttributeValue, representing the item blocking current request from acquiring the lock.
     */
    protected Map<String, AttributeValue> getItemBlockingUpdate(Map<String, AttributeValue> keyValues, TSDMetrics metrics) {
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedWorkflowIdGenerator.getItemBlockingUpdate");
        try {
            GetItemResult result = DynamoDBHelper.getItemAttributesFromTable(dynamoDBClient, workflowIdGenerationTableName, keyValues);
            return result.getItem();
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Extracts the workflowId from the updated item result.
     * @param updatedItem result of the update operation.
     * @return WorkflowId from the updated item.
     */
    private long getWorkflowIdFromUpdatedItem(UpdateItemResult updatedItem) {
        Map<String, AttributeValue> oldAttributeValues = updatedItem.getAttributes();
        AttributeValue newWorkflowIdAttributeValue = oldAttributeValues.get(WORKFLOW_ID_KEY);
        return Long.parseLong(newWorkflowIdAttributeValue.getN());
    }
    
    /**
     * Protected for unit-tests.
     */
    protected int getUpdateRetrySleepMillis() {
        return DDB_UPDATE_RETRY_SLEEP_MILLIS_MULTIPLIER;
    }
}
