package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
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
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Sets;

public class DDBBasedGetMitigationInfoHandlerTest {
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
    public void testGetMitigationInstanceStatus() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedGetMitigationInfoHandler mitigationInfoHandler = new DDBBasedGetMitigationInfoHandler(dynamoDBClient, domain);
        DDBBasedGetMitigationInfoHandler spiedMitigationInfoHandler = spy(mitigationInfoHandler);
        
        Map<String, AttributeValue> keyValues = new HashMap<>();
        keyValues.put(DDBBasedGetMitigationInfoHandler.LOCATION_KEY, new AttributeValue("AMS1"));
        keyValues.put(DDBBasedGetMitigationInfoHandler.MITIGATION_STATUS_KEY, new AttributeValue("DEPLOYED"));
        
        List<Map<String, AttributeValue>> inputList = new ArrayList<>();
        inputList.add(keyValues);
        
        QueryResult queryResult = new QueryResult();
        queryResult.setItems(inputList);
        List<MitigationInstanceStatus> expectedResult = new ArrayList<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("AMS1");
        status.setMitigationStatus("DEPLOYED");
        expectedResult.add(status);
        
        doReturn(queryResult).when(spiedMitigationInfoHandler).queryMitigationsInDDB(any(QueryRequest.class), any(TSDMetrics.class));
        List<MitigationInstanceStatus> list = spiedMitigationInfoHandler.getMitigationInstanceStatus("POP_ROUTER", Long.valueOf("1"), tsdMetrics);
        assertEquals(list, expectedResult);
    }
    
    public void testGenerateQueryRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedGetMitigationInfoHandler mitigationInfoHandler = new DDBBasedGetMitigationInfoHandler(dynamoDBClient, domain);
        
        QueryRequest queryRequest = mitigationInfoHandler.generateQueryRequest(generateAttributesToGet(), generateKeyCondition(), generateKeyCondition(), "MitigationTable", true, "random_index", generateStartKey());
        assertEquals(queryRequest, createQueryRequest()); 
    }
    
    public static QueryRequest createQueryRequest() {
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setAttributesToGet(generateAttributesToGet());
        queryRequest.setTableName("MitigationTable");
        queryRequest.setConsistentRead(true);
        queryRequest.setKeyConditions(generateKeyCondition());
        queryRequest.setQueryFilter(generateKeyCondition());
        queryRequest.setExclusiveStartKey(generateStartKey());
        queryRequest.setIndexName("random_index");
        
        return queryRequest;
    }
    
    public static Map<String, Condition> generateKeyCondition() {
        Map<String, Condition> keyCondition = new HashMap<>();

        Set<AttributeValue> keyValues = new HashSet<>();
        AttributeValue keyValue = new AttributeValue("Route53");
        keyValues.add(keyValue);
        
        return keyCondition;
    }
    
    public static Map<String, AttributeValue> generateStartKey() {
        Map<String, AttributeValue> item = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue("Mitigation-1");
        item.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, attributeValue);
        
        
        return item;
    }
    
    public static Set<String> generateAttributesToGet() {
        Set<String> attributeToGet = Sets.newHashSet(DDBBasedGetMitigationInfoHandler.DEVICE_WORKFLOW_ID_KEY);
        return attributeToGet;
    }
    
}
