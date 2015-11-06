package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;

import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.MitigationRequestItemCreator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.status.helper.ActiveMitigationsStatusHelper;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Lists;

import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.*;

public class DDBBasedListMitigationsHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private final static String domain = "beta";
    
    private static DDBBasedListMitigationsHandler listHandler;
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static RequestTableTestHelper requestTableTestHelper;
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
        listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, 
                domain, mock(ActiveMitigationsStatusHelper.class));
        requestTableTestHelper = new RequestTableTestHelper(dynamodb, domain);
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        String tableName = MitigationRequestsModel.getInstance().getTableName(domain);
        CreateTableRequest request = MitigationRequestsModel.getInstance().getCreateTableRequest(tableName);
        if (Tables.doesTableExist(dynamoDBClient, tableName)) {
            dynamoDBClient.deleteTable(tableName);
        }
        dynamoDBClient.createTable(request);
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatus() {
        DDBBasedListMitigationsHandler requestInfoHandler = mock(DDBBasedListMitigationsHandler.class);
        
        GetRequestStatusRequest request = createRequestStatusRequest();
        Map<String, AttributeValue> key= requestInfoHandler.generateRequestInfoKey(request.getDeviceName(), Long.valueOf(request.getJobId()));
        
        when(requestInfoHandler.getRequestInDDB(key, tsdMetrics)).thenReturn(createGetItemResultForMitigationNameAndRequestStatus());
        
        when(requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics)).thenCallRealMethod();
        MitigationNameAndRequestStatus nameAndStatus = requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
        assertEquals(nameAndStatus.getMitigationName(), "Mitigation-1");
        assertEquals(nameAndStatus.getRequestStatus(), "DEPLOYED");
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatusWhenNoRequests() {
        DDBBasedListMitigationsHandler requestInfoHandler = mock(DDBBasedListMitigationsHandler.class);
        
        GetRequestStatusRequest request = createRequestStatusRequest();
        Map<String, AttributeValue> key= requestInfoHandler.generateRequestInfoKey(request.getDeviceName(), Long.valueOf(request.getJobId()));
        
        when(requestInfoHandler.getRequestInDDB(key, tsdMetrics)).thenReturn(createEmptyGetItemResultForMitigationNameAndRequestStatus());
        
        when(requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics)).thenCallRealMethod();
        try {
            requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("Could not find an item for the requested"));
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testGetMitigationNameAndRequestStatusWhenEmptyParameters() {
        DDBBasedListMitigationsHandler requestInfoHandler = mock(DDBBasedListMitigationsHandler.class);
        
        GetRequestStatusRequest request = new GetRequestStatusRequest();
        Map<String, AttributeValue> key= requestInfoHandler.generateRequestInfoKey(request.getDeviceName(), Long.valueOf(request.getJobId()));
        
        when(requestInfoHandler.getRequestInDDB(key, tsdMetrics)).thenReturn(createEmptyGetItemResultForMitigationNameAndRequestStatus());
        
        when(requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics)).thenCallRealMethod();
        requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatusWhenWrongTemplate() {
        DDBBasedListMitigationsHandler requestInfoHandler = mock(DDBBasedListMitigationsHandler.class);
        
        GetRequestStatusRequest request = createRequestStatusRequest();
        Map<String, AttributeValue> key= requestInfoHandler.generateRequestInfoKey(request.getDeviceName(), Long.valueOf(request.getJobId()));
        
        when(requestInfoHandler.getRequestInDDB(key, tsdMetrics)).thenReturn(createGetItemResultWithWrongTemplateForMitigationNameAndRequestStatus());
        
        when(requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics)).thenCallRealMethod();
        try {
            requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("associated with a different template than requested"));
        }
    }
    
    @Test
    public void testGetActiveMitigationsForServiceNoActive() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain, mock(ActiveMitigationsStatusHelper.class));
        DDBBasedListMitigationsHandler spiedListHandler = spy(listHandler);
        
        doReturn(new QueryResult().withCount(0)).when(spiedListHandler).queryRequestsInDDB(any(QueryRequest.class), any(TSDMetrics.class));
        List<ActiveMitigationDetails> list = spiedListHandler.getActiveMitigationsForService("Route53", "foo", new ArrayList<String>(), tsdMetrics);
        assertEquals(list, new ArrayList<ActiveMitigationDetails>());
    }
    
    /**
     * Test if we query the DDB with the right keys and parse the results to form the correct MitigationDescription instance. 
     */
    @Test
    public void testGetMitigationDescription() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain, mock(ActiveMitigationsStatusHelper.class));
        
        String deviceName = "testDevice";
        long workflowId = 5;
        String deviceScope = "testScope";
        List<String> locations = Lists.newArrayList("TST1", "TST2", "TST3");
        String mitigationName = "testMitigation";
        String mitigationTemplate = "testTemplate";
        int mitigationVersion = 3;
        int numPreDeployChecks = 0;
        int numPostDeployChecks = 0;
        String reaped = "false";
        List<String> relatedTickets = Lists.newArrayList("tt/1", "tt/2");
        long requestDate = Long.parseLong("1410896163");
        String requestType = RequestType.CreateRequest.name();
        String swfRunId = "22Ans97eYkYe5pHRfsPirITXUHwowfqWWjfHQ2sKquXzI=";
        String serviceName = ServiceName.Route53;
        String toolName = "LookoutMitigationServiceUI";
        long updateWorkflowId = 0;
        String userDescription = "randomString";
        String userName = "testUser";
        String workflowStatus = "SUCCEEDED";
        
        Map<String, AttributeValue> keys = new HashMap<>();
        AttributeValue attributeValue = new AttributeValue(deviceName);
        keys.put(MitigationRequestsModel.DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(workflowId));
        keys.put(MitigationRequestsModel.WORKFLOW_ID_KEY, attributeValue);
        
        MitigationDefinition mitigationDefinition = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String mitigationDefinitionJsonString = jsonDataConverter.toData(mitigationDefinition);
        
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase()).withKey(keys);
        
        GetItemResult getItemResult = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        getItemResult.setItem(item);
        item.put(MitigationRequestsModel.DEVICE_NAME_KEY, new AttributeValue(deviceName));
        item.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(workflowId)));
        item.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(deviceScope));
        item.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(locations));
        item.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue(mitigationName));
        item.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue(mitigationTemplate));
        item.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(mitigationVersion)));
        item.put(MitigationRequestsModel.NUM_PRE_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.NUM_POST_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.REAPED_FLAG_KEY, new AttributeValue(reaped));
        item.put(MitigationRequestsModel.RELATED_TICKETS_KEY, new AttributeValue().withSS(relatedTickets));
        item.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(requestDate)));
        item.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(requestType));
        item.put(MitigationRequestsModel.SWF_RUN_ID_KEY, new AttributeValue(swfRunId));
        item.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(serviceName));
        item.put(MitigationRequestsModel.TOOL_NAME_KEY, new AttributeValue(toolName));
        item.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(updateWorkflowId)));
        item.put(MitigationRequestsModel.USER_DESCRIPTION_KEY, new AttributeValue(userDescription));
        item.put(MitigationRequestsModel.USER_NAME_KEY, new AttributeValue(userName));
        item.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(workflowStatus));
        item.put(MitigationRequestsModel.MITIGATION_DEFINITION_KEY, new AttributeValue(mitigationDefinitionJsonString));
        getItemResult.setItem(item);
        when(dynamoDBClient.getItem(getItemRequest)).thenReturn(getItemResult);
        
        MitigationRequestDescription description = listHandler.getMitigationRequestDescription(deviceName, workflowId, tsdMetrics);
        assertEquals(description.getDeviceName(), deviceName);
        assertEquals(description.getJobId(), workflowId);
        assertEquals(description.getDeviceScope(), deviceScope);
        assertEquals(description.getMitigationName(), mitigationName);
        assertEquals(description.getMitigationTemplate(), mitigationTemplate);
        assertEquals(description.getMitigationVersion(), mitigationVersion);
        assertEquals(description.getNumPreDeployChecks(), numPreDeployChecks);
        assertEquals(description.getNumPostDeployChecks(), numPostDeployChecks);
        assertEquals(description.getMitigationActionMetadata().getRelatedTickets(), relatedTickets);
        assertEquals(description.getMitigationActionMetadata().getDescription(), userDescription);
        assertEquals(description.getMitigationActionMetadata().getUser(), userName);
        assertEquals(description.getMitigationActionMetadata().getToolName(), toolName);
        assertEquals(description.getRequestDate(), requestDate);
        assertEquals(description.getRequestType(), requestType);
        assertEquals(description.getServiceName(), serviceName);
        assertEquals(description.getUpdateJobId(), updateWorkflowId);
        assertEquals(description.getRequestStatus(), workflowStatus);
        assertEquals(description.getMitigationDefinition(), mitigationDefinition);
    }

    /**
     * Test to ensure we pass the right set of keys/queryFilters when querying DDB for fetching mitigation description.
     * Also checks if we correctly parse the QueryResult to wrap it into List of MitigationRequestDescription instances.
     */
    @Test
    public void testGetMitigationDescriptionsForMitigation() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain, mock(ActiveMitigationsStatusHelper.class));
        
        String deviceName = "testDevice";
        long workflowId = 5;
        String deviceScope = "testScope";
        List<String> locations = Lists.newArrayList("TST1", "TST2", "TST3");
        String mitigationName = "testMitigation";
        String mitigationTemplate = "testTemplate";
        int mitigationVersion = 3;
        int numPreDeployChecks = 0;
        int numPostDeployChecks = 0;
        String reaped = "false";
        List<String> relatedTickets = Lists.newArrayList("tt/1", "tt/2");
        long requestDate = Long.parseLong("1410896163");
        String requestType = RequestType.CreateRequest.name();
        String swfRunId = "22Ans97eYkYe5pHRfsPirITXUHwowfqWWjfHQ2sKquXzI=";
        String serviceName = ServiceName.Route53;
        String toolName = "LookoutMitigationServiceUI";
        long updateWorkflowId = 0;
        String userDescription = "randomString";
        String userName = "testUser";
        String workflowStatus = "SUCCEEDED";
        
        // Setup the keys to check.
        Map<String, Condition> keyConditions = new HashMap<>();
        AttributeValue value = new AttributeValue(deviceName);
        Condition deviceCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        keyConditions.put(MitigationRequestsModel.DEVICE_NAME_KEY, deviceCondition);
        
        value = new AttributeValue(mitigationName);
        Condition mitigationNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        keyConditions.put(MitigationRequestsModel.MITIGATION_NAME_KEY, mitigationNameCondition);
        
        // Setup the queryFilters to check.
        Map<String, Condition> queryFilter = new HashMap<>();
        value = new AttributeValue(serviceName);
        Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        queryFilter.put(MitigationRequestsModel.SERVICE_NAME_KEY, serviceNameCondition);
        
        // Restrict results to this deviceName
        value = new AttributeValue(deviceScope);
        Condition deviceScopeCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        queryFilter.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, deviceScopeCondition);
        
        // Restrict results to only requests which are "active" (i.e. there hasn't been a subsequent workflow updating the actions of this workflow)
        value = new AttributeValue().withN("0");
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        queryFilter.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, condition);
        
        // Ignore delete requests.
        value = new AttributeValue(RequestType.DeleteRequest.name());
        Condition requestTypeCondition = new Condition().withComparisonOperator(ComparisonOperator.NE).withAttributeValueList(value);
        queryFilter.put(MitigationRequestsModel.REQUEST_TYPE_KEY, requestTypeCondition);
        
        MitigationDefinition mitigationDefinition = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String mitigationDefinitionJsonString = jsonDataConverter.toData(mitigationDefinition);
        
        QueryResult queryResult = new QueryResult();
        
        List<Map<String, AttributeValue>> listOfItems = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(MitigationRequestsModel.DEVICE_NAME_KEY, new AttributeValue(deviceName));
        item.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(workflowId)));
        item.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(deviceScope));
        item.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(locations));
        item.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue(mitigationName));
        item.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue(mitigationTemplate));
        item.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(mitigationVersion)));
        item.put(MitigationRequestsModel.NUM_PRE_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.NUM_POST_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.REAPED_FLAG_KEY, new AttributeValue(reaped));
        item.put(MitigationRequestsModel.RELATED_TICKETS_KEY, new AttributeValue().withSS(relatedTickets));
        item.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(requestDate)));
        item.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(requestType));
        item.put(MitigationRequestsModel.SWF_RUN_ID_KEY, new AttributeValue(swfRunId));
        item.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(serviceName));
        item.put(MitigationRequestsModel.TOOL_NAME_KEY, new AttributeValue(toolName));
        item.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(updateWorkflowId)));
        item.put(MitigationRequestsModel.USER_DESCRIPTION_KEY, new AttributeValue(userDescription));
        item.put(MitigationRequestsModel.USER_NAME_KEY, new AttributeValue(userName));
        item.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(workflowStatus));
        item.put(MitigationRequestsModel.MITIGATION_DEFINITION_KEY, new AttributeValue(mitigationDefinitionJsonString));
        listOfItems.add(item);
        queryResult.setItems(listOfItems);
        
        when(dynamoDBClient.query(any(QueryRequest.class))).thenReturn(queryResult);
        
        List<MitigationRequestDescription> descriptions = listHandler.getActiveMitigationRequestDescriptionsForMitigation(serviceName, deviceName, deviceScope, mitigationName, tsdMetrics);
        
        assertEquals(descriptions.size(), 1);
        MitigationRequestDescription description = descriptions.get(0);
        assertEquals(description.getDeviceName(), deviceName);
        assertEquals(description.getJobId(), workflowId);
        assertEquals(description.getDeviceScope(), deviceScope);
        assertEquals(description.getMitigationName(), mitigationName);
        assertEquals(description.getMitigationTemplate(), mitigationTemplate);
        assertEquals(description.getMitigationVersion(), mitigationVersion);
        assertEquals(description.getNumPreDeployChecks(), numPreDeployChecks);
        assertEquals(description.getNumPostDeployChecks(), numPostDeployChecks);
        assertEquals(description.getMitigationActionMetadata().getRelatedTickets(), relatedTickets);
        assertEquals(description.getMitigationActionMetadata().getDescription(), userDescription);
        assertEquals(description.getMitigationActionMetadata().getUser(), userName);
        assertEquals(description.getMitigationActionMetadata().getToolName(), toolName);
        assertEquals(description.getRequestDate(), requestDate);
        assertEquals(description.getRequestType(), requestType);
        assertEquals(description.getServiceName(), serviceName);
        assertEquals(description.getUpdateJobId(), updateWorkflowId);
        assertEquals(description.getRequestStatus(), workflowStatus);
        assertEquals(description.getMitigationDefinition(), mitigationDefinition);
    }
    
    private GetRequestStatusRequest createRequestStatusRequest() {
        GetRequestStatusRequest request = new GetRequestStatusRequest();
        request.setDeviceName("POP_ROUTER");
        request.setMitigationTemplate("Router_RateLimit_Route53Customer");
        request.setJobId(Long.valueOf("1"));
        request.setServiceName("Route53");
        return request;
    }
    
    private GetItemResult createGetItemResultForMitigationNameAndRequestStatus() {
        Map<String, AttributeValue> item = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue("Mitigation-1");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, attributeValue);
        attributeValue = new AttributeValue("DEPLOYED");
        item.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, attributeValue);
        attributeValue = new AttributeValue("Router_RateLimit_Route53Customer");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, attributeValue);
        GetItemResult result = new GetItemResult();
        result.setItem(item);
        
        return result;
    }
    
    private GetItemResult createEmptyGetItemResultForMitigationNameAndRequestStatus() {
        Map<String, AttributeValue> item = new HashMap<>();
        
        GetItemResult result = new GetItemResult();
        result.setItem(item);
        
        return result;
    }
    
    private GetItemResult createGetItemResultWithWrongTemplateForMitigationNameAndRequestStatus() {
        Map<String, AttributeValue> item = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue("Mitigation-1");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, attributeValue);
        attributeValue = new AttributeValue("DEPLOYED");
        item.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, attributeValue);
        attributeValue = new AttributeValue("Wrong_Template");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, attributeValue);
        GetItemResult result = new GetItemResult();
        result.setItem(item);
    
        return result;
    }
    
    /**
     * Test to ensure we pass the right set of keys/queryFilters when querying DDB for fetching mitigation description.
     * Also checks if we correctly parse the QueryResult to wrap it into List of MitigationRequestDescription instances.
     */
    @Test
    public void testGetInProgressRequestsDescription() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain, mock(ActiveMitigationsStatusHelper.class));
        
        String deviceName = "testDevice";
        long workflowId = 5;
        String deviceScope = "testScope";
        List<String> locations = Lists.newArrayList("TST1", "TST2", "TST3");
        String mitigationName = "testMitigation";
        String mitigationTemplate = "testTemplate";
        int mitigationVersion = 3;
        int numPreDeployChecks = 0;
        int numPostDeployChecks = 0;
        String reaped = "false";
        List<String> relatedTickets = Lists.newArrayList("tt/1", "tt/2");
        long requestDate = Long.parseLong("1410896163");
        String requestType = RequestType.CreateRequest.name();
        String swfRunId = "22Ans97eYkYe5pHRfsPirITXUHwowfqWWjfHQ2sKquXzI=";
        String serviceName = ServiceName.Route53;
        String toolName = "LookoutMitigationServiceUI";
        long updateWorkflowId = 0;
        String userDescription = "randomString";
        String userName = "testUser";
        String workflowStatus = "SUCCEEDED";
        
        // Setup the keys to check.
        Map<String, Condition> keyConditions = new HashMap<>();
        AttributeValue value = new AttributeValue(deviceName);
        Condition deviceCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, deviceCondition);
        
        value = new AttributeValue().withN("0");
        Condition uneditedCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        keyConditions.put(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY, uneditedCondition);
        
        // Setup the queryFilters to check.
        Map<String, Condition> queryFilter = new HashMap<>();
        value = new AttributeValue(serviceName);
        Condition serviceNameCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        queryFilter.put(DDBBasedRequestStorageHandler.SERVICE_NAME_KEY, serviceNameCondition);
        
        value = new AttributeValue(WorkflowStatus.RUNNING);
        Condition runningCondition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(value);
        queryFilter.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, runningCondition);
        
        MitigationDefinition mitigationDefinition = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String mitigationDefinitionJsonString = jsonDataConverter.toData(mitigationDefinition);
        
        QueryRequest queryRequest = new QueryRequest().withTableName(MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase())
                                                      .withConsistentRead(true)
                                                      .withKeyConditions(keyConditions)
                                                      .withQueryFilter(queryFilter)
                                                      .withExclusiveStartKey(null)
                                                      .withIndexName(DDBBasedRequestStorageHandler.UNEDITED_MITIGATIONS_LSI_NAME);
        
        QueryResult queryResult = new QueryResult();
        
        List<Map<String, AttributeValue>> listOfItems = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(MitigationRequestsModel.DEVICE_NAME_KEY, new AttributeValue(deviceName));
        item.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(workflowId)));
        item.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(deviceScope));
        item.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(locations));
        item.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue(mitigationName));
        item.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue(mitigationTemplate));
        item.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(mitigationVersion)));
        item.put(MitigationRequestsModel.NUM_PRE_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.NUM_POST_DEPLOY_CHECKS_KEY, new AttributeValue().withN(String.valueOf("0")));
        item.put(MitigationRequestsModel.REAPED_FLAG_KEY, new AttributeValue(reaped));
        item.put(MitigationRequestsModel.RELATED_TICKETS_KEY, new AttributeValue().withSS(relatedTickets));
        item.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(requestDate)));
        item.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(requestType));
        item.put(MitigationRequestsModel.SWF_RUN_ID_KEY, new AttributeValue(swfRunId));
        item.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(serviceName));
        item.put(MitigationRequestsModel.TOOL_NAME_KEY, new AttributeValue(toolName));
        item.put(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(updateWorkflowId)));
        item.put(MitigationRequestsModel.USER_DESCRIPTION_KEY, new AttributeValue(userDescription));
        item.put(MitigationRequestsModel.USER_NAME_KEY, new AttributeValue(userName));
        item.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(workflowStatus));
        item.put(MitigationRequestsModel.MITIGATION_DEFINITION_KEY, new AttributeValue(mitigationDefinitionJsonString));
        listOfItems.add(item);
        queryResult.setCount(1);
        queryResult.setItems(listOfItems);
        
        when(dynamoDBClient.query(queryRequest)).thenReturn(queryResult);
        
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, tsdMetrics);
        
        assertEquals(descriptions.size(), 1);
        MitigationRequestDescriptionWithLocations descriptionWithLocations = descriptions.get(0);
        MitigationRequestDescription description = descriptionWithLocations.getMitigationRequestDescription();
        assertEquals(description.getDeviceName(), deviceName);
        assertEquals(description.getJobId(), workflowId);
        assertEquals(description.getDeviceScope(), deviceScope);
        assertEquals(description.getMitigationName(), mitigationName);
        assertEquals(description.getMitigationTemplate(), mitigationTemplate);
        assertEquals(description.getMitigationVersion(), mitigationVersion);
        assertEquals(description.getNumPreDeployChecks(), numPreDeployChecks);
        assertEquals(description.getNumPostDeployChecks(), numPostDeployChecks);
        assertEquals(description.getMitigationActionMetadata().getRelatedTickets(), relatedTickets);
        assertEquals(description.getMitigationActionMetadata().getDescription(), userDescription);
        assertEquals(description.getMitigationActionMetadata().getUser(), userName);
        assertEquals(description.getMitigationActionMetadata().getToolName(), toolName);
        assertEquals(description.getRequestDate(), requestDate);
        assertEquals(description.getRequestType(), requestType);
        assertEquals(description.getServiceName(), serviceName);
        assertEquals(description.getUpdateJobId(), updateWorkflowId);
        assertEquals(description.getRequestStatus(), workflowStatus);
        assertEquals(description.getMitigationDefinition(), mitigationDefinition);
        assertEquals(descriptionWithLocations.getLocations(), locations);
    }

    /**
     * Test all history can be retrieved.
     */
    @Test
    public void testGetMitigationHistory() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        // validate all history can be retrieved.
        Integer startVersion = 15;
        Integer maxNumberOfHistoryEntriesToFetch = 20;
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, startVersion, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(versionCounts, descs.size());
        for (int v = versionCounts; v >= 1; --v) {
            assertEquals(v, descs.get(versionCounts - v).getMitigationVersion());
        }
    }

    /**
     * Test all history can be retrieved, when startVersion is null
     */
    @Test
    public void testGetMitigationHistoryMissingStartVersion() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        // validate all history can be retrieved, when startVersion is null
        Integer maxNumberOfHistoryEntriesToFetch = 20;
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(versionCounts, descs.size());
        for (int v = versionCounts; v >= 1; --v) {
            assertEquals(v, descs.get(versionCounts - v).getMitigationVersion());
        }
    }

    /**
     * Test maxNumberOfHistoryEntriesToFetch
     */
    @Test
    public void testGetMitigationHistoryMaxNumberOfHistoryEntriesToFetch() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        // validate all history can be retrieved, when startVersion is null
        int maxNumberOfHistoryEntriesToFetch = 4;
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(maxNumberOfHistoryEntriesToFetch, descs.size());
        for (int i = 0; i < maxNumberOfHistoryEntriesToFetch; ++i) {
            assertEquals(versionCounts - i, descs.get(i).getMitigationVersion());
        }
    }

    /**
     * Test startVersion
     */
    @Test
    public void testGetMitigationHistoryStartVersion() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        // validate all history can be retrieved, when startVersion is null
        Integer startVersion = 8;
        int maxNumberOfHistoryEntriesToFetch = 4;
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, startVersion, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(maxNumberOfHistoryEntriesToFetch, descs.size());
        for (int i = 1; i <= maxNumberOfHistoryEntriesToFetch; ++i) {
            assertEquals(startVersion - i, descs.get(i - 1).getMitigationVersion());
        }
    }
    
    /**
     * Test mitigation does not exist.
     */
    @Test(expected = MissingMitigationException400.class)
    public void testGetMitigationHistoryMitigationNotExist() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, "nonExistMitigation", null, 20, tsdMetrics);
        assertEquals(0, descs.size());
    }
    
    /**
     * Test query filter, request Type
     */
    @Test
    public void testGetMitigationHistoryMitigationRequestType() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }
        
        itemCreator.setRequestType(RequestType.DeleteRequest.name());
        itemCreator.setMitigationVersion(11);
        itemCreator.setWorkflowId(10011);
        itemCreator.addItem();
        
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.setMitigationVersion(12);
        itemCreator.setWorkflowId(10012);
        itemCreator.addItem();
        
        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 2, tsdMetrics);

        assertEquals(2, descs.size());
        assertEquals(12, descs.get(0).getMitigationVersion());
        assertEquals(10, descs.get(1).getMitigationVersion());
    }
    
    /**
     * Test query filter, service name
     */
    @Test
    public void testGetMitigationHistoryMitigationServiceName() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        itemCreator.setServiceName(ServiceName.Blackhole);
        itemCreator.setMitigationVersion(11);
        itemCreator.setWorkflowId(10011);
        itemCreator.addItem();

        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationVersion());
    }
    
    /**
     * Test query filter, device scope
     */
    @Test
    public void testGetMitigationHistoryMitigationDeviceScope() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        itemCreator.setDeviceScope("NRT");
        itemCreator.setMitigationVersion(11);
        itemCreator.setWorkflowId(10011);
        itemCreator.addItem();

        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationVersion());
    }
    
    /**
     * Test query filter, device name
     */
    @Test
    public void testGetMitigationHistoryMitigationDeviceName() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        itemCreator.setDeviceName(DeviceName.POP_ROUTER.name());
        itemCreator.setMitigationVersion(11);
        itemCreator.setWorkflowId(10011);
        itemCreator.addItem();

        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationVersion());
    }
    
    /**
     * Test query filter, workflow status
     */
    @Test
    public void testGetMitigationHistoryMitigationWorkflowStatus() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        int versionCounts = 10;
        // add versions 1 ~ versionCounts
        for (int v = 1; v <= versionCounts; ++v) {
            itemCreator.setMitigationVersion(v);
            itemCreator.setWorkflowId(v + 10000);
            itemCreator.addItem();
        }

        itemCreator.setWorkflowStatus(WorkflowStatus.INDETERMINATE);
        itemCreator.setMitigationVersion(11);
        itemCreator.setWorkflowId(10011);
        itemCreator.addItem();

        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.setMitigationVersion(12);
        itemCreator.setWorkflowId(10012);
        itemCreator.addItem();

        itemCreator.setWorkflowStatus(WorkflowStatus.PARTIAL_SUCCESS);
        itemCreator.setMitigationVersion(13);
        itemCreator.setWorkflowId(10013);
        itemCreator.addItem();

        itemCreator.setWorkflowStatus(WorkflowStatus.SUCCEEDED);
        itemCreator.setMitigationVersion(14);
        itemCreator.setWorkflowId(10014);
        itemCreator.addItem();

        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(15);
        itemCreator.setWorkflowId(10015);
        itemCreator.addItem();

        // validate all history can be retrieved, when startVersion is null
        List<MitigationRequestDescription> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 3, tsdMetrics);

        assertEquals(3, descs.size());
        assertEquals(15, descs.get(0).getMitigationVersion());
        assertEquals(14, descs.get(1).getMitigationVersion());
        assertEquals(10, descs.get(2).getMitigationVersion());
    }

    /**
     * Test it correctly get a mitigation definition.
     */
    @Test
    public void testGetMitigationDefinition() {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        itemCreator.setLocations(locations);
        int workflowId = 10000;

        // 2 records that will be fetched.
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.addItem();
        
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.addItem();
        
        // different service name
        itemCreator.setServiceName("otherService");
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.addItem();
 
        // same name other device
        itemCreator.setServiceName(serviceName);
        itemCreator.setDeviceName("anotherDevice");
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.addItem();
        
        // same device, different name
        itemCreator.setDeviceName(deviceName);
        itemCreator.setMitigationName("anotherMitigation");
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.addItem();
        
        // same device, name, different version
        itemCreator.setMitigationName(mitigationName);
        itemCreator.setMitigationVersion(3);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.addItem();
 
        // delete request
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setRequestType(RequestType.DeleteRequest.name());
        itemCreator.addItem();
        
        // workflow status is not succeeded or running
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.addItem();
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.INDETERMINATE);
        itemCreator.addItem();
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.PARTIAL_SUCCESS);
        itemCreator.addItem();
       
        MitigationRequestDescriptionWithLocations desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName,
                1, tsdMetrics);

        assertEquals(1, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10001, desc.getMitigationRequestDescription().getJobId());
        
        desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 2, tsdMetrics);

        assertEquals(2, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10002, desc.getMitigationRequestDescription().getJobId());
    }

    /**
     * get definition on a non-exist mitigation, should throw exception
     */
    @Test(expected = MissingMitigationException400.class)
    public void testGetMitigationDefinitionNonExistMitigation() {
        listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 1, tsdMetrics);
    }
    
    /**
     * Test get mitigation info can correctly handle deleted mitigation
     */
    @Test(expected = MissingMitigationException400.class)
    public void testGetMitigationInfoOnDeletedMitigation() {
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        // create a mitigation
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(10000);
        itemCreator.setUpdateWorkflowId(10001);
        itemCreator.addItem();
        // edit the mitigation
        itemCreator.setRequestType(RequestType.EditRequest.name());
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(10001);
        itemCreator.setUpdateWorkflowId(10002);
        itemCreator.addItem();
        // delete the mitigation
        itemCreator.setRequestType(RequestType.DeleteRequest.name());
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(10002);
        itemCreator.setUpdateWorkflowId(0);
        itemCreator.addItem();
        
        listHandler.getActiveMitigationRequestDescriptionsForMitigation(
                serviceName, deviceName, deviceScope, mitigationName, tsdMetrics);
    }
    
    /**
     * Test get mitigation info can correctly handle ongoing deployment on a certain mitigation
     */
    @Test
    public void testGetMitigationInfoWithOngoingDeployment() {
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        // create a mitigation
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(10000);
        itemCreator.setUpdateWorkflowId(0);
        itemCreator.addItem();
        // edit the mitigation
        itemCreator.setRequestType(RequestType.EditRequest.name());
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(10001);
        itemCreator.setUpdateWorkflowId(0);
        itemCreator.addItem();
       
        List<MitigationRequestDescription> descs = listHandler.getActiveMitigationRequestDescriptionsForMitigation(
                serviceName, deviceName, deviceScope, mitigationName, tsdMetrics);
        
        assertEquals(2, descs.size());
        assertTrue((1 == descs.get(0).getMitigationVersion()) && (2 == descs.get(1).getMitigationVersion()) || 
                (2 == descs.get(0).getMitigationVersion()) && (1 == descs.get(1).getMitigationVersion()));
    }
 
}
