package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static com.amazon.lookout.ddb.model.MitigationRequestsModel.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;

import org.joda.time.DateTime;
import org.mockito.stubbing.Stubber;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Sets;

public class RequestTableTestHelper {
    private final Table requestsTable;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
    public RequestTableTestHelper(DynamoDB dynamodb, String domain) {
        this.requestsTable = dynamodb.getTable(MitigationRequestsModel.getInstance().getTableName(domain));
    }
    
    @Data
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
        itemCreator.setTable(requestsTable);
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

    public MitigationRequestDescriptionWithLocations getRequestDescriptionWithLocations(String deviceName, long workflowId) {
        Item item = requestsTable.getItem(DDBRequestSerializer.getPrimaryKey(deviceName, workflowId));
        return DDBRequestSerializer.convertToRequestDescriptionWithLocations(item);
    }
    
    public ItemCollection<QueryOutcome> getRequestsWithName(String deviceName, String mitigationName) {
        Index index = requestsTable.getIndex(MitigationRequestsModel.MITIGATION_NAME_LSI);
        return index.query(new QuerySpec()
                        .withHashKey(new KeyAttribute(MitigationRequestsModel.DEVICE_NAME_KEY, deviceName))
                        .withRangeKeyCondition(new RangeKeyCondition(MitigationRequestsModel.MITIGATION_NAME_KEY).eq(mitigationName)));
    }
    
    public void setUpdateWorkflowId(String deviceName, long workflowId, long updateWorkflowId) {
        requestsTable.updateItem(new UpdateItemSpec()
            .withPrimaryKey(DDBRequestSerializer.getPrimaryKey(deviceName, workflowId))
            .withExpected(new Expected(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).eq(
                    MitigationRequestsModel.UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS))
            .withAttributeUpdate(new AttributeUpdate(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).put(updateWorkflowId)));
    }
    
    public void setWorkflowStatus(String deviceName, long workflowId, String status) {
        requestsTable.updateItem(new UpdateItemSpec()
            .withPrimaryKey(DDBRequestSerializer.getPrimaryKey(deviceName, workflowId))
            .withExpected(new Expected(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).eq(
                    MitigationRequestsModel.UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS))
            .withAttributeUpdate(new AttributeUpdate(MitigationRequestsModel.WORKFLOW_STATUS_KEY).put(status)));
    }
    
    public MitigationRequestDescriptionWithLocations validateRequestInDDB(
            MitigationModificationRequest request, MitigationDefinition mitigationDefinition, int mitigationVersion,
            Set<String> locations, long workflowId) 
    {
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        MitigationRequestDescriptionWithLocations storedDefinition = 
                getRequestDescriptionWithLocations(
                        deviceNameAndScope.getDeviceName().toString(), workflowId);
        
        MitigationRequestDescription storedDescription = storedDefinition.getMitigationRequestDescription();
        assertEquals(toJson(mitigationDefinition), toJson(storedDescription.getMitigationDefinition()));
        assertEquals(toJson(request.getMitigationActionMetadata()), toJson(storedDescription.getMitigationActionMetadata()));
        assertEquals(request.getMitigationTemplate(), storedDescription.getMitigationTemplate());
        assertEquals(mitigationVersion, storedDescription.getMitigationVersion());
        if (workflowId != -1) {
            assertEquals(workflowId, storedDescription.getJobId());
        }
        
        assertEquals(locations, new HashSet<>(storedDefinition.getLocations()));
        
        return storedDefinition;
    }
    
    private static String toJson(Object obj) {
        try {
            return writer.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void whenAnyPut(DDBBasedRequestStorageHandler handler, Stubber stubber) {
        stubber.when(handler).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
}
