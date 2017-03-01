package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.activities.model.MitigationInstanceInfo;
import com.amazon.lookout.mitigation.activities.model.MitigationInstanceSchedulingStatus;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

public class DDBBasedMitigationInstanceHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedMitigationInstanceHandler.class);

    private final String mitigationInstancesTableName;

    public static final String DEVICE_WORKFLOW_ID_HASH_KEY = MitigationInstancesModel.DEVICE_WORKFLOW_ID_HASH_KEY;
    public static final String LOCATION_RANGE_KEY = MitigationInstancesModel.LOCATION_KEY;
    public static final String SERVICE_NAME_KEY = MitigationInstancesModel.SERVICE_NAME_KEY;
    public static final String DEVICE_NAME_KEY = MitigationInstancesModel.DEVICE_NAME_KEY;
    public static final String SCHEDULING_STATUS_KEY = MitigationInstancesModel.SCHEDULING_STATUS_KEY;
    public static final String BLOCKING_DEVICE_WORKFLOW_ID_KEY = MitigationInstancesModel.BLOCKING_DEVICE_WORKFLOW_ID_KEY;

    private final AmazonDynamoDB dynamoDBClient;

    public DDBBasedMitigationInstanceHandler(@Nonnull AmazonDynamoDB dynamoDBClient, @Nonnull String domain) {
        this.dynamoDBClient = dynamoDBClient;
        this.mitigationInstancesTableName = MitigationInstancesModel.getInstance().getTableName(domain);
    }
    
    
    /**
     * get mitigation instance attributes
     * @param workflowId : workflow Id
     * @param deviceName : device name
     * @param location : location
     * @param attributesToGet : list of attributes to get, if missing, will return all attributes
     * @return map of attribute key to value
     * @throws AmazonClientException
     * @throws AmazonServiceException
     */
    public Map<String, AttributeValue> getMitigationInstanceAttributes(final long workflowId, final @Nonnull String deviceName, final @Nonnull String location, 
                                                                          final @Nullable List<String> attributesToGet) throws AmazonClientException, AmazonServiceException {

        Map<String, AttributeValue> key = new HashMap<>();
        String deviceWorflowId = MitigationInstancesModel.getDeviceWorkflowId(deviceName, workflowId);
        key.put(DEVICE_WORKFLOW_ID_HASH_KEY, new AttributeValue().withS(deviceWorflowId));
        key.put(LOCATION_RANGE_KEY, new AttributeValue().withS(location));

        GetItemRequest getItemRequest = new GetItemRequest()
            .withTableName(mitigationInstancesTableName)
            .withKey(key)
            .withAttributesToGet(attributesToGet)
            .withConsistentRead(true);

        LOG.debug(String.format("Attempting to fulfill request for getMitigationInstanceAttributes: %s.", getItemRequest));
        GetItemResult result = dynamoDBClient.getItem(getItemRequest);
        return result.getItem();
    }   
    
    /**
     * Get blockingWorkflowId and Scheduling status of a mitigation instance 
     * @param workflowId : workflow Id
     * @param deviceName : device name
     * @param location : location
     * @return blockingWorkflowId and Scheduling status in string
     * 
     */
    public MitigationInstanceSchedulingStatus getMitigationInstanceSchedulingStatus(final long workflowId, final @Nonnull String deviceName, final @Nonnull String location)
    {
        Validate.isTrue(workflowId > 0);
        Validate.notEmpty(deviceName);
        Validate.notEmpty(location);

        try {
            Map<String, AttributeValue> mitigationInstanceItem = this.getMitigationInstanceAttributes(workflowId, deviceName, location,
                    Arrays.asList(DEVICE_WORKFLOW_ID_HASH_KEY,
                            LOCATION_RANGE_KEY, SCHEDULING_STATUS_KEY,
                            BLOCKING_DEVICE_WORKFLOW_ID_KEY));
            return instanceStatusFromItem(mitigationInstanceItem);
        } catch(Exception ex) {
            LOG.error(String.format("Failed to get instance scheduling status for workflow %s and device %s at location %s.", workflowId, deviceName, location), ex);
        }
        return null;
    }
    
    private static MitigationInstanceSchedulingStatus instanceStatusFromItem(Map<String,AttributeValue> item) {
        String deviceWorkflowId = item.get(MitigationInstancesModel.DEVICE_WORKFLOW_ID_HASH_KEY).getS();
        String location = item.get(MitigationInstancesModel.LOCATION_RANGE_KEY).getS();
        MitigationInstanceInfo mitigationInstanceInfo = MitigationInstanceInfo.parseDeviceWorkflowId(deviceWorkflowId, location);
        SchedulingStatus schedulingStatus = SchedulingStatus.valueOf(item.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY).getS());
        AttributeValue blockingDeviceWorkflowIdAttrValue = item.get(MitigationInstancesModel.BLOCKING_DEVICE_WORKFLOW_ID_KEY);
        String blockingDeviceWorkflowId = blockingDeviceWorkflowIdAttrValue == null ? null
                : blockingDeviceWorkflowIdAttrValue.getS();
        MitigationInstanceInfo blockingInstanceInfo = StringUtils.isBlank(blockingDeviceWorkflowId) ? null
                : MitigationInstanceInfo.parseDeviceWorkflowId(blockingDeviceWorkflowId, location);
        return new MitigationInstanceSchedulingStatus(mitigationInstanceInfo, schedulingStatus, blockingInstanceInfo, null);
    }

}
