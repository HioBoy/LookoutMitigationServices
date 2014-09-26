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
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DDBBasedListMitigationsHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private final String domain = "beta";
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
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
            MitigationNameAndRequestStatus nameAndStatus = requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
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
        MitigationNameAndRequestStatus nameAndStatus = requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
    }
    
    @Test
    public void testGetMitigationNameAndRequestStatusWhenWrongTemplate() {
        DDBBasedListMitigationsHandler requestInfoHandler = mock(DDBBasedListMitigationsHandler.class);
        
        GetRequestStatusRequest request = createRequestStatusRequest();
        Map<String, AttributeValue> key= requestInfoHandler.generateRequestInfoKey(request.getDeviceName(), Long.valueOf(request.getJobId()));
        
        when(requestInfoHandler.getRequestInDDB(key, tsdMetrics)).thenReturn(createGetItemResultWithWrongTemplateForMitigationNameAndRequestStatus());
        
        when(requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics)).thenCallRealMethod();
        try {
            MitigationNameAndRequestStatus nameAndStatus = requestInfoHandler.getMitigationNameAndRequestStatus(request.getDeviceName(), request.getMitigationTemplate(), Long.valueOf(request.getJobId()), tsdMetrics);
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("associated with a different template than requested"));
        }
    }
    
    @Test
    public void testGetActiveMitigationsForServiceNoActive() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain);
        DDBBasedListMitigationsHandler spiedListHandler = spy(listHandler);
        
        doReturn(new QueryResult().withCount(0)).when(spiedListHandler).queryRequestsInDDB(any(QueryRequest.class), any(TSDMetrics.class));
        List<ActiveMitigationDetails> list = spiedListHandler.getActiveMitigationsForService("Route53", "foo", new ArrayList(), tsdMetrics);
        assertEquals(list, new ArrayList<ActiveMitigationDetails>());
    }
    
    /**
     * Test if we query the DDB with the right keys and parse the results to form the correct MitigationDescription instance. 
     */
    @Test
    public void testGetMitigationDescription() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain);
        
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
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain);
        
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
        
        Set<String> attributes = Sets.newHashSet(MitigationRequestsModel.getAttributeNamesForRequestTable());
        
        MitigationDefinition mitigationDefinition = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String mitigationDefinitionJsonString = jsonDataConverter.toData(mitigationDefinition);
        
        QueryRequest queryRequest = new QueryRequest().withAttributesToGet(attributes)
                                                      .withTableName(MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase())
                                                      .withConsistentRead(true)
                                                      .withKeyConditions(keyConditions)
                                                      .withQueryFilter(queryFilter)
                                                      .withExclusiveStartKey(null)
                                                      .withIndexName(MitigationRequestsModel.MITIGATION_NAME_LSI);
        
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
        
        when(dynamoDBClient.query(queryRequest)).thenReturn(queryResult);
        
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
        DDBBasedListMitigationsHandler listHandler = new DDBBasedListMitigationsHandler(dynamoDBClient, domain);
        
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
        
        Set<String> attributes = Sets.newHashSet(MitigationRequestsModel.getAttributeNamesForRequestTable());
        
        MitigationDefinition mitigationDefinition = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String mitigationDefinitionJsonString = jsonDataConverter.toData(mitigationDefinition);
        
        QueryRequest queryRequest = new QueryRequest().withAttributesToGet(attributes)
                                                      .withTableName(MitigationRequestsModel.MITIGATION_REQUESTS_TABLE_NAME_PREFIX + domain.toUpperCase())
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
}
