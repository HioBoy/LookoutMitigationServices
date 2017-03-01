package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static com.amazon.lookout.ddb.model.MitigationInstancesModel.*;
import lombok.Data;
import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class InstanceTableTestHelper {
    private final Table instanceTable;
    public InstanceTableTestHelper(DynamoDB dynamodb, String domain) {
        this.instanceTable = dynamodb.getTable(MitigationInstancesModel.getInstance().getTableName(domain));
    }
    
    @Data
    static class MitigationInstanceItemCreator {
        private Table table;
        private String deviceName;
        private String deviceWorkflowId;
        private String mitigationTemplate;
        private String location;
        private String mitigationName;
        private String mitigationStatus;
        private String schedulingStatus;
        private String serviceName;
        private Long mitigationVersion;
        private String blockingDeviceWorkflowId;
        private String workflowId;

        
        void addItem() {
            Item item = new Item();
            item.withString(DEVICE_NAME_KEY, deviceName);
            item.withString(SERVICE_NAME_KEY, serviceName);
            item.withString(MITIGATION_NAME_KEY, mitigationName);
            item.withNumber(MITIGATION_VERSION_KEY, mitigationVersion);
            item.withString(MITIGATION_TEMPLATE_KEY, mitigationTemplate);
            item.withString(DEVICE_WORKFLOW_ID_KEY, MitigationInstancesModel.getDeviceWorkflowId(deviceName, workflowId));
            item.withString(MITIGATION_STATUS_KEY, mitigationStatus);
            item.withString(LOCATION_KEY, location);
            item.withString(SCHEDULING_STATUS_KEY, schedulingStatus);
            if (blockingDeviceWorkflowId != null && !blockingDeviceWorkflowId.isEmpty()) {
                item.withString(BLOCKING_DEVICE_WORKFLOW_ID_KEY, blockingDeviceWorkflowId);
            }
            table.putItem(item);
        }
    }

    public MitigationInstanceItemCreator getItemCreator(String deviceName, String serviceName) {
        MitigationInstanceItemCreator itemCreator = new MitigationInstanceItemCreator();
        itemCreator.setTable(instanceTable);
        itemCreator.setDeviceName(deviceName);
        itemCreator.setMitigationTemplate(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer);
        itemCreator.setServiceName(serviceName);
        itemCreator.setSchedulingStatus(schedulingStatus);
        itemCreator.setMitigationName(mitigationName);
        itemCreator.setWorkflowId(workflowId);
        itemCreator.setLocation(location);
        itemCreator.setMitigationStatus(MitigationStatus.EDIT_SUCCEEDED);
        itemCreator.setMitigationVersion(mitigationVersion);
        return itemCreator;
    }

    public static final String deviceName = DeviceName.BLACKWATCH_BORDER.toString();
    public static final String serviceName = ServiceName.AWS;
    public static final String mitigationName = "mitigation1";
    public static final String mitigationTemplate = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    public static final String location = "testLocation";
    public static final Long  mitigationVersion = 1l;

    public static final String schedulingStatus = SchedulingStatus.COMPLETED.name();
    public static final String workflowId = "1235";

    

}
