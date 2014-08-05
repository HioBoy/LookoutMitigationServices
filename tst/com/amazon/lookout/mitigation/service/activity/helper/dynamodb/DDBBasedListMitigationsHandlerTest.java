package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.activities.model.MitigationMetadata;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
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
        
        doReturn(new QueryResult()).when(spiedListHandler).queryRequestsInDDB(any(QueryRequest.class), any(TSDMetrics.class));
        List<ActiveMitigationDetails> list = spiedListHandler.getActiveMitigationsForService("Route53", "foo", new ArrayList(), tsdMetrics);
        assertEquals(list, new ArrayList<ActiveMitigationDetails>());
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

    private ListActiveMitigationsForServiceRequest createListActiveMitigationsForServiceRequest() {
        ListActiveMitigationsForServiceRequest request = new ListActiveMitigationsForServiceRequest(); 
        request.setDeviceName("POP_ROUTER");
        request.setLocations(Arrays.asList("AMS1", "AMS50"));
        request.setServiceName("Route53");
        
        return request;
    }
    
    private GetItemResult createGetItemResult() {
        Map<String, AttributeValue> item = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue("lookout");
        item.put(DDBBasedRequestStorageHandler.USERNAME_KEY, attributeValue);
        attributeValue = new AttributeValue("blah blah");
        item.put(DDBBasedRequestStorageHandler.USER_DESC_KEY, attributeValue);
        attributeValue = new AttributeValue("tool");
        item.put(DDBBasedRequestStorageHandler.TOOL_NAME_KEY, attributeValue);
        attributeValue = new AttributeValue();
        attributeValue.setSS(Arrays.asList("1234", "2345"));
        item.put(DDBBasedRequestStorageHandler.RELATED_TICKETS_KEY, attributeValue);
        attributeValue = new AttributeValue("[\"com.amazon.lookout.mitigation.service.MitigationDefinition\",{\"constraint\":[\"com.amazon.lookout.mitigation.service.SimpleConstraint\",{\"attributeName\":\"SOURCE_ASN\",\"attributeValues\":[\"java.util.ArrayList\",[\"1234\",\"4567\",\"4192\"]]}],\"action\":null}]");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY, attributeValue);
        GetItemResult result = new GetItemResult();
        result.setItem(item);
        attributeValue = new AttributeValue("template");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, attributeValue);
        return result;
    }
    
    private MitigationMetadata createMitigationMetadata() {
        DataConverter jsonDataConverter = new JsonDataConverter();
        MitigationMetadata metaData = new MitigationMetadata();
        MitigationActionMetadata data = new MitigationActionMetadata();
        data.setDescription("blah blah");
        data.setRelatedTickets(Arrays.asList("1234", "2345"));
        data.setToolName("tool");
        data.setUser("lookout");
        
        metaData.setMitigationActionMetadata(data);
        metaData.setRequestDate(Long.valueOf("1403552269925"));
        metaData.setMitigationDefinition(jsonDataConverter.fromData("[\"com.amazon.lookout.mitigation.service.MitigationDefinition\",{\"constraint\":[\"com.amazon.lookout.mitigation.service.SimpleConstraint\",{\"attributeName\":\"SOURCE_ASN\",\"attributeValues\":[\"java.util.ArrayList\",[\"1234\",\"4567\",\"4192\"]]}],\"action\":null}]", MitigationDefinition.class));
        metaData.setMitigationTemplate("template");
        
        return metaData;
    }
    
}
