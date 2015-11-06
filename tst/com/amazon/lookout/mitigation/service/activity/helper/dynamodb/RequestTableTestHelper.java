package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static com.amazon.lookout.ddb.model.MitigationRequestsModel.*;

import java.util.Set;

import lombok.Setter;

import org.joda.time.DateTime;

import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.common.collect.Sets;

public class RequestTableTestHelper {
    public RequestTableTestHelper(DynamoDB dynamodb, String domain) {
        this.dynamodb = dynamodb;
        this.domain = domain;
    }

    private final DynamoDB dynamodb;
    private final String domain;
    
    @Setter
    static class MitigationRequestItemCreator {
        private Table table;
        private String deviceName;
        private Integer workflowId;
        private String mitigationTemplate;
        private String workflowStatus;
        private String serviceName;
        private Long requestDate;
        private Integer updateWorkflowId;
        private String mitigationName;
        private Integer mitigationVersion;
        private String requestType;
        private Set<String> locations;

        private String userName;
        private String userDesc;
        private String toolName;
        private String deviceScope;

        void addItem() {
            Item item = new Item();
            item.withString(DEVICE_NAME_KEY, deviceName);
            item.withNumber(WORKFLOW_ID_KEY, workflowId);
            item.withString(MITIGATION_TEMPLATE_NAME_KEY, mitigationTemplate);
            item.withString(WORKFLOW_STATUS_KEY, workflowStatus);
            item.withString(SERVICE_NAME_KEY, serviceName);
            item.withNumber(REQUEST_DATE_IN_MILLIS_KEY, requestDate);
            item.withNumber(UPDATE_WORKFLOW_ID_KEY, updateWorkflowId);
            item.withString(MITIGATION_NAME_KEY, mitigationName);
            item.withNumber(MITIGATION_VERSION_KEY, mitigationVersion);
            item.withString(REQUEST_TYPE_KEY, requestType);
            item.withString(USER_NAME_KEY, userName);
            item.withString(USER_DESCRIPTION_KEY, userDesc);
            item.withString(TOOL_NAME_KEY, toolName);
            item.withString(DEVICE_SCOPE_KEY, deviceScope);
            if (locations != null) {
                item.withStringSet(LOCATIONS_KEY, locations);
            }
            table.putItem(item);
        }
    }

    public MitigationRequestItemCreator getItemCreator(String deviceName, String serviceName, String mitigationName,
            String deviceScope) {
        MitigationRequestItemCreator itemCreator = new MitigationRequestItemCreator();
        itemCreator.setTable(dynamodb.getTable(MitigationRequestsModel.getInstance().getTableName(domain)));
        itemCreator.setDeviceName(deviceName);
        itemCreator.setMitigationTemplate(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer);
        itemCreator.setWorkflowStatus(WorkflowStatus.SUCCEEDED);
        itemCreator.setServiceName(serviceName);
        itemCreator.setRequestDate(DateTime.now().getMillis());
        itemCreator.setUpdateWorkflowId(1000);
        itemCreator.setMitigationName(mitigationName);
        itemCreator.setRequestType(RequestType.EditRequest.name());
        itemCreator.setDeviceScope(deviceScope);
        itemCreator.setLocations(locations);

        itemCreator.setUserName("abc");
        itemCreator.setUserDesc("abc");
        itemCreator.setToolName("A");

        return itemCreator;
    }

    public static final String deviceName = DeviceName.BLACKWATCH_BORDER.toString();
    public static final String serviceName = ServiceName.AWS;
    public static final String deviceScope = DeviceScope.GLOBAL.toString();
    public static final String mitigationName = "mitigation1";
    public static final Set<String> locations = Sets.newHashSet("AMS1", "ARN1");
    public static final MitigationDefinition mitigationDefinition = new MitigationDefinition();
    public static final String mitigationTemplate = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    static {
        mitigationDefinition.setAction(new DropAction());
        mitigationDefinition.setConstraint(new BlackWatchConfigBasedConstraint());
    }

}
