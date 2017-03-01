package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.InstanceTableTestHelper.MitigationInstanceItemCreator;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.MitigationRequestItemCreator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
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
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;

import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.*;

public class DDBBasedListMitigationsHandlerTest {
    private static final String domain = "unit-test";
    
    private static DDBBasedListMitigationsHandler listHandler;
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static RequestTableTestHelper requestTableTestHelper;
    private static InstanceTableTestHelper instanceTableTestHelper;
    
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
        listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, 
                domain, mock(ActiveMitigationsStatusHelper.class));
        requestTableTestHelper = new RequestTableTestHelper(dynamodb, domain);
        instanceTableTestHelper = new InstanceTableTestHelper(dynamodb, domain);
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        String tableName = MitigationRequestsModel.getInstance().getTableName(domain);
        CreateTableRequest request = MitigationRequestsModel.getInstance().getCreateTableRequest(tableName);
        TableUtils.deleteTableIfExists(dynamoDBClient, new DeleteTableRequest(tableName));
        dynamoDBClient.createTable(request);
        
        String instanceTableName = MitigationInstancesModel.getInstance().getTableName(domain);
        request = MitigationInstancesModel.getInstance().getCreateTableRequest(instanceTableName);
        TableUtils.deleteTableIfExists(dynamoDBClient, new DeleteTableRequest(instanceTableName));
        dynamoDBClient.createTable(request);
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatus() {
        MitigationRequestItemCreator itemCreator = 
                requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        itemCreator.setWorkflowStatus("DEPLOYED");
        itemCreator.setWorkflowId(10);
        itemCreator.setMitigationVersion(1);
        itemCreator.addItem();
        
        MitigationNameAndRequestStatus nameAndStatus = listHandler.getMitigationNameAndRequestStatus(
                itemCreator.getDeviceName(), itemCreator.getMitigationTemplate(), 10, tsdMetrics);
        assertEquals(nameAndStatus.getMitigationName(), mitigationName);
        assertEquals(nameAndStatus.getRequestStatus(), "DEPLOYED");
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatusWhenNoRequests() {
        try {
            listHandler.getMitigationNameAndRequestStatus(
                    DeviceName.POP_ROUTER.name(), MitigationTemplate.Router_CountMode_Route53Customer, 11, tsdMetrics);
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("Could not find an item for the requested"));
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testGetMitigationNameAndRequestStatusWhenEmptyParameters() {
        listHandler.getMitigationNameAndRequestStatus(null, null, 0, tsdMetrics);
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatusWhenWrongTemplate() {
        MitigationRequestItemCreator itemCreator = 
                requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        itemCreator.setWorkflowStatus("DEPLOYED");
        itemCreator.setWorkflowId(10);
        itemCreator.setMitigationVersion(1);
        itemCreator.addItem();
        
        try {
            listHandler.getMitigationNameAndRequestStatus(
                    itemCreator.getDeviceName(), itemCreator.getMitigationTemplate(), 10, tsdMetrics);
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("associated with a different template than requested"));
        }
    }
    
    @Test
    public void testGetActiveMitigationsForServiceNoActive() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain, mock(ActiveMitigationsStatusHelper.class));
        DDBBasedListMitigationsHandler spiedListHandler = spy(listHandler);
        
        doReturn(new QueryResult().withCount(0)).when(spiedListHandler).queryDynamoDBWithRetries(any(QueryRequest.class), any(TSDMetrics.class));
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
        
        MitigationDefinition mitigationDefinition = RequestTestHelper.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
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
        
        MitigationDefinition mitigationDefinition = RequestTestHelper.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
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
        
        List<MitigationRequestDescription> descriptions = listHandler.getMitigationRequestDescriptionsForMitigation(serviceName, deviceName, deviceScope, mitigationName, tsdMetrics);
        
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
    
    /**
     * Test fetch the running request succeed
     */
    @Test
    public void testGetInProgressRequestsDescription() {
        
        //create request in the  mitigation request table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 1);
        MitigationRequestDescriptionWithLocations descriptionWithLocations = descriptions.get(0);
        MitigationRequestDescription description = descriptionWithLocations.getMitigationRequestDescription();
        assertEquals(description.getDeviceName(), deviceName);
        assertEquals(description.getDeviceScope(), deviceScope);
        assertEquals(description.getMitigationName(), mitigationName);
        assertEquals(description.getMitigationTemplate(), mitigationTemplate);
        assertEquals(description.getServiceName(), serviceName);
    }
    
    /**
     * Test fetch the running request, no location filter. 
     */
    @Test
    public void testGetInProgressRequestsDescription_NoLocationFilter() {
        
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = null;
        itemCreator.setLocations(locations);
        itemCreator.setUpdateWorkflowId(0);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(2);
        itemCreator.addItem();
        
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(3);
        itemCreator.setWorkflowId(3);
        itemCreator.addItem();
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 2);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 3);
        description = descriptions.get(1).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 1);
    }
    

    /**
     * Test fetch the running request, failed because there is no running request. 
     */
    @Test
    public void testGetInProgressRequestsDescription_NoRunningRequest() {
        
        //create request in the  mitigation request table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 0);
    }
    
    /**
     * Test fetch the running request, failed due to location in not in the request
     */
    @Test
    public void testGetInProgressRequestsDescription_LocationNotInRequest() {
        
        //create request in the  mitigation request table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TSTxxx");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 0);
    }
    
    /**
     * Test fetch the running request, we should return all of them in reverse order (workflowId)
     */
    @Test
    public void testGetInProgressRequestsDescription_WorkflowID_ReverseScan() {
        
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(2);
        itemCreator.addItem();
        
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(3);
        itemCreator.setWorkflowId(3);
        itemCreator.addItem();
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 3);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 3);
        description = descriptions.get(1).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 2);
        description = descriptions.get(2).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 1);
    }
    
    
    /**
     * Test fetch the running request, three requests, one failed in the middle.
     * we should only return the latest one
     */
    @Test
    public void testGetInProgressRequestsDescription_FailRequestInMiddle() {
        
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(2);
        itemCreator.addItem();
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId("test#1");
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(3);
        itemCreator.setWorkflowId(3);
        itemCreator.addItem();
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);
        assertEquals(descriptions.size(), 2);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 3);
        description = descriptions.get(1).getMitigationRequestDescription();
        assertEquals(description.getJobId(),1);
    }
    
    @Test
    public void testGetInProgressRequestsDescription_KeepSearchingAfterBlockedAndCompletedInstance() {
        
        DDBBasedListMitigationsHandler spylistHandler = spy(listHandler);
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        
        QueryResult queryResult = new QueryResult();
        QueryResult queryResult2 = new QueryResult();
        itemCreator.setLocations(locations);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>() ;
        lastEvaluatedKey.put("test", new AttributeValue("test"));

        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.RUNNING, "1", "0", "1"));
        items.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.FAILED, "2", "0", "2"));
        queryResult.setItems(items);
        queryResult.setCount(2);
        queryResult.setLastEvaluatedKey(lastEvaluatedKey);
        
        List<Map<String, AttributeValue>> items2 = new ArrayList<>();
        items2.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.RUNNING, "3", "0", "3"));
        queryResult2.setItems(items2);
        queryResult2.setCount(1);
        queryResult2.setLastEvaluatedKey(null);
        doReturn(queryResult).doReturn(queryResult2).when(spylistHandler).queryDynamoDBWithRetries(any(QueryRequest.class), any(TSDMetrics.class));
        
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId("test#1");
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        List<MitigationRequestDescriptionWithLocations> descriptions = spylistHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);
        assertEquals(descriptions.size(), 2);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 1);
        description = descriptions.get(1).getMitigationRequestDescription();
        assertEquals(description.getJobId(),3);
    }
    
    @Test
    public void testGetInProgressRequestsDescription_StopSearchingAfterCompletedInstance() {
        
        DDBBasedListMitigationsHandler spylistHandler = spy(listHandler);
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2", "TST3"));
        List<String> queryLocations = Lists.newArrayList("TST1");
        
        QueryResult queryResult = new QueryResult();
        QueryResult queryResult2 = new QueryResult();
        itemCreator.setLocations(locations);
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>() ;
        lastEvaluatedKey.put("test", new AttributeValue("test"));

        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.RUNNING, "1", "0", "1"));
        items.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.FAILED, "2", "0", "2"));
        queryResult.setItems(items);
        queryResult.setCount(2);
        queryResult.setLastEvaluatedKey(lastEvaluatedKey);
        
        List<Map<String, AttributeValue>> items2 = new ArrayList<>();
        items2.add(itemCreator.generateQueryResultItem(deviceName, WorkflowStatus.RUNNING, "3", "0", "3"));
        queryResult2.setItems(items2);
        queryResult2.setCount(1);
        queryResult2.setLastEvaluatedKey(null);
        doReturn(queryResult).doReturn(queryResult2).when(spylistHandler).queryDynamoDBWithRetries(any(QueryRequest.class), any(TSDMetrics.class));
        
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId(null);
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        List<MitigationRequestDescriptionWithLocations> descriptions = spylistHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);
        assertEquals(descriptions.size(), 1);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 1);
    }
    
    /**
     * Test fetch the running request
     */
    @Test
    public void testGetInProgressRequestsDescription_Multiple_Locations() {
        
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        Set<String> locations = new HashSet<String>(Lists.newArrayList("TST1", "TST2"));
        List<String> queryLocations = Lists.newArrayList("TST2", "TST1");
        itemCreator.setLocations(locations);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(1);
        itemCreator.addItem();
       
        locations = new HashSet<String>(Lists.newArrayList("TST3", "TST2"));
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(2);
        itemCreator.addItem();
        
        List<MitigationRequestDescriptionWithLocations> descriptions = listHandler.getOngoingRequestsDescription(serviceName, deviceName, queryLocations, tsdMetrics);        
        assertEquals(descriptions.size(), 2);
        MitigationRequestDescription description = descriptions.get(0).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 2);
        description = descriptions.get(1).getMitigationRequestDescription();
        assertEquals(description.getJobId(), 1);
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, startVersion, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(versionCounts, descs.size());
        for (int v = versionCounts; v >= 1; --v) {
            assertEquals(v, descs.get(versionCounts - v).getMitigationRequestDescription().getMitigationVersion());
            assertEquals(new ArrayList<>(locations), descs.get(versionCounts - v).getLocations());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(versionCounts, descs.size());
        for (int v = versionCounts; v >= 1; --v) {
            assertEquals(v, descs.get(versionCounts - v).getMitigationRequestDescription().getMitigationVersion());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(maxNumberOfHistoryEntriesToFetch, descs.size());
        for (int i = 0; i < maxNumberOfHistoryEntriesToFetch; ++i) {
            assertEquals(versionCounts - i, descs.get(i).getMitigationRequestDescription().getMitigationVersion());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, startVersion, maxNumberOfHistoryEntriesToFetch, tsdMetrics);

        assertEquals(maxNumberOfHistoryEntriesToFetch, descs.size());
        for (int i = 1; i <= maxNumberOfHistoryEntriesToFetch; ++i) {
            assertEquals(startVersion - i, descs.get(i - 1).getMitigationRequestDescription().getMitigationVersion());
        }
    }
    
    /**
     * Test mitigation does not exist.
     */
    @Test
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
        assertTrue(listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, "nonExistMitigation", null, 20, tsdMetrics).isEmpty());
    }
    
    /**
     * Test query filter, all create, edit, delete request Type should be returned
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 2, tsdMetrics);

        assertEquals(2, descs.size());
        assertEquals(12, descs.get(0).getMitigationRequestDescription().getMitigationVersion());
        assertEquals(11, descs.get(1).getMitigationRequestDescription().getMitigationVersion());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationRequestDescription().getMitigationVersion());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationRequestDescription().getMitigationVersion());
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
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 1, tsdMetrics);

        assertEquals(1, descs.size());
        assertEquals(10, descs.get(0).getMitigationRequestDescription().getMitigationVersion());
    }
    
    /**
     * Test query filter on workflow status, filter out Failed request only
     * @throws JsonProcessingException 
     */
    @Test
    public void testGetMitigationHistoryMitigationWorkflowStatus() throws JsonProcessingException {
        // create history for a mitigation in ddb table
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(deviceName, serviceName, mitigationName, deviceScope);

        // create one request for each workflow status
        // and find the failed workflow status version 
        int version = 1;
        int versionOfFailedStatus = 0;
        for (String status : WorkflowStatus.values()) {
            ++version;
            if (status.equals(WorkflowStatus.FAILED)) {
                versionOfFailedStatus = version;
            }
            itemCreator.setWorkflowStatus(status);
            itemCreator.setMitigationVersion(version);
            itemCreator.setWorkflowId(10000 + version);
            itemCreator.addItem();
       }

        int expectedRequestsCount = WorkflowStatus.values().length - 1;

        // validate history is correctly retrieved without Failed status request
        List<MitigationRequestDescriptionWithLocations> descs = listHandler.getMitigationHistoryForMitigation(serviceName, deviceName,
                deviceScope, mitigationName, null, 10, tsdMetrics);

        assertEquals(expectedRequestsCount, descs.size());
        int validateVersion = version;
        for (int i = 0; i < expectedRequestsCount; ++i, --validateVersion) {
            if (versionOfFailedStatus == validateVersion) {
                // skip failed status request
                validateVersion -= 1;
            }
            assertEquals(String.format("Failed request version %d, failed to validate request %s", versionOfFailedStatus,
                    prettyWriter.writeValueAsString(descs.get(i).getMitigationRequestDescription())),
                    validateVersion, descs.get(i).getMitigationRequestDescription().getMitigationVersion());
        }
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

        // 4 records that will be fetched.
        itemCreator.setMitigationVersion(1);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.SUCCEEDED);
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.addItem();
        
        itemCreator.setMitigationVersion(2);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.RUNNING);
        itemCreator.setRequestType(RequestType.EditRequest.name());
        itemCreator.addItem();
  
        // delete request
        itemCreator.setMitigationVersion(3);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.PARTIAL_SUCCESS);
        itemCreator.setRequestType(RequestType.DeleteRequest.name());
        itemCreator.addItem();
  
        // rollback request
        itemCreator.setMitigationVersion(4);
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.INDETERMINATE);
        itemCreator.setRequestType(RequestType.RollbackRequest.name());
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
       
        // workflow status is FAILED
        itemCreator.setRequestType(RequestType.CreateRequest.name());
        itemCreator.setWorkflowId(++workflowId);
        itemCreator.setWorkflowStatus(WorkflowStatus.FAILED);
        itemCreator.addItem();
      
        // validate first request
        MitigationRequestDescriptionWithLocations desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName,
                1, tsdMetrics);
        assertEquals(1, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10001, desc.getMitigationRequestDescription().getJobId());
        assertEquals(RequestType.CreateRequest.name(), desc.getMitigationRequestDescription().getRequestType());
        assertEquals(WorkflowStatus.SUCCEEDED, desc.getMitigationRequestDescription().getRequestStatus());
        
        // validate second request
        desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 2, tsdMetrics);
        assertEquals(2, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10002, desc.getMitigationRequestDescription().getJobId());
        assertEquals(RequestType.EditRequest.name(), desc.getMitigationRequestDescription().getRequestType());
        assertEquals(WorkflowStatus.RUNNING, desc.getMitigationRequestDescription().getRequestStatus());

        // validate 3rd request
        desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 3, tsdMetrics);
        assertEquals(3, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10003, desc.getMitigationRequestDescription().getJobId());
        assertEquals(RequestType.DeleteRequest.name(), desc.getMitigationRequestDescription().getRequestType());
        assertEquals(WorkflowStatus.PARTIAL_SUCCESS, desc.getMitigationRequestDescription().getRequestStatus());
         
        // validate 4th request
        desc = listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 4, tsdMetrics);
        assertEquals(4, desc.getMitigationRequestDescription().getMitigationVersion());
        assertEquals(deviceName, desc.getMitigationRequestDescription().getDeviceName());
        assertEquals(mitigationName, desc.getMitigationRequestDescription().getMitigationName());
        assertEquals(10004, desc.getMitigationRequestDescription().getJobId());
        assertEquals(RequestType.RollbackRequest.name(), desc.getMitigationRequestDescription().getRequestType());
        assertEquals(WorkflowStatus.INDETERMINATE, desc.getMitigationRequestDescription().getRequestStatus());
    }

    /**
     * get definition on a non-exist mitigation, should throw exception
     */
    @Test(expected = MissingMitigationVersionException404.class)
    public void testGetMitigationDefinitionNonExistMitigation() {
        listHandler.getMitigationDefinition(deviceName, serviceName, mitigationName, 1, tsdMetrics);
    }
}
