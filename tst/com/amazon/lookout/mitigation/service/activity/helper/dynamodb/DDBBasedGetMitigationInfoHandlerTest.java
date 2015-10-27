package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static com.amazon.lookout.ddb.model.MitigationInstancesModel.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Setter;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.MissingLocationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.google.common.collect.Sets;

public class DDBBasedGetMitigationInfoHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private static final String domain = "beta";
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static DDBBasedGetMitigationInfoHandler mitigationInfoHandler;
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
        mitigationInfoHandler = new DDBBasedGetMitigationInfoHandler(dynamoDBClient, domain);
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        String tableName = MitigationInstancesModel.getTableName(domain);
        CreateTableRequest request = MitigationInstancesModel.getCreateTableRequest(tableName);
        if (Tables.doesTableExist(dynamoDBClient, tableName)) {
            dynamoDBClient.deleteTable(tableName);
        }
        dynamoDBClient.createTable(request);
    }
    
    @Test
    public void testGetMitigationInstanceStatus() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedGetMitigationInfoHandler mitigationInfoHandler = new DDBBasedGetMitigationInfoHandler(dynamoDBClient, domain);
        DDBBasedGetMitigationInfoHandler spiedMitigationInfoHandler = spy(mitigationInfoHandler);
        
        Map<String, AttributeValue> keyValues = new HashMap<>();
        keyValues.put(DDBBasedGetMitigationInfoHandler.LOCATION_KEY, new AttributeValue("AMS1"));
        keyValues.put(DDBBasedGetMitigationInfoHandler.MITIGATION_STATUS_KEY, new AttributeValue("DEPLOYED"));
        keyValues.put(DDBBasedGetMitigationInfoHandler.DEPLOYMENT_HISTORY_KEY, new AttributeValue(Arrays.asList("event1", "event2")));
        
        List<Map<String, AttributeValue>> inputList = new ArrayList<>();
        inputList.add(keyValues);
        
        QueryResult queryResult = new QueryResult();
        queryResult.setItems(inputList);
        List<MitigationInstanceStatus> expectedResult = new ArrayList<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("AMS1");
        status.setMitigationStatus("DEPLOYED");
        status.setDeploymentHistory(Arrays.asList("event1", "event2"));
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

    @Setter
    private class MitigationInstanceItemCreator {
        Table table;

        String deviceWorkflowId;
        String location;
        String mitigationStatus;
        String blockingDeviceWorkflowId;
        String schedulingStatus;
        String serviceName;
        String deviceName;
        String createDate;

        String mitigationName;
        Integer mitigationVersion;

        void addItem() {
            Item item = new Item();

            item.withString(DEVICE_WORKFLOW_ID_HASH_KEY, deviceWorkflowId);
            item.withString(LOCATION_RANGE_KEY, location);
            item.withString(MITIGATION_STATUS_KEY, mitigationStatus);
            item.withString(BLOCKING_DEVICE_WORKFLOW_ID, blockingDeviceWorkflowId);
            item.withString(SCHEDULING_STATUS_KEY, schedulingStatus);
            item.withString(SERVICE_NAME_KEY, serviceName);
            item.withString(DEVICE_NAME_KEY, deviceName);
            item.withString(CREATE_DATE_KEY, createDate);

            item.withString(MITIGATION_NAME_KEY, mitigationName);
            item.withNumber(MITIGATION_VERSION_KEY, mitigationVersion);

            table.putItem(item);
        }
    }

    static final String deviceName = DeviceName.BLACKWATCH_BORDER.toString();
    static final String serviceName = ServiceName.AWS;
    static final String blockingDeviceWorkflowId = "BLACKWATCH_BORDER#110023";
    static final String mitigationStatus = "EDIT_SUCCEEDED";
    static final String schedulingStatus = "COMPLETED";
    static final String location = "G-IAD55";

    static final String mitigationName = "mitigation1";

    private MitigationInstanceItemCreator getItemCreator(String deviceName, String serviceName, String mitigationName,
            String location, String mitigationStatus, String blockingDeviceWorkflowId, String schedulingStatus) {
        MitigationInstanceItemCreator itemCreator = new MitigationInstanceItemCreator();
        itemCreator.setTable(dynamodb.getTable(MitigationInstancesModel.getTableName(domain)));

        itemCreator.setLocation(location);
        itemCreator.setMitigationStatus(mitigationStatus);
        itemCreator.setBlockingDeviceWorkflowId(blockingDeviceWorkflowId);
        itemCreator.setSchedulingStatus(schedulingStatus);
        itemCreator.setDeviceName(deviceName);
        itemCreator.setServiceName(serviceName);
        itemCreator.setMitigationName(mitigationName);

        return itemCreator;
    }

    /**
     * Test all deployment history are retrieved.
     */
    @Test
    public void testGetLocationDeploymentHistory() {
        // create deployment history for a location in ddb table
        MitigationInstanceItemCreator itemCreator = getItemCreator(deviceName, serviceName, mitigationName, location,
                mitigationStatus, blockingDeviceWorkflowId, schedulingStatus);

        // add deployment history
        int deploymentCount = 10;
        DateTime dateTime = DateTime.now();
        for (int v = 1; v <= deploymentCount; ++v) {
            itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, v + 10000));
            itemCreator.setCreateDate(dateTime.plusMinutes(v).toString(CREATE_DATE_FORMATTER));
            itemCreator.setMitigationVersion(v);
            itemCreator.addItem();
        }

        // validate all deployment history are retrieved.
        List<LocationDeploymentInfo> instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName,
                serviceName, location, 100, null, tsdMetrics);

        assertEquals(deploymentCount, instanceStatuses.size());
        for (int i = 0; i < deploymentCount; ++i) {
            assertEquals(deploymentCount - i, instanceStatuses.get(i).getMitigationVersion());
        }
    }

    /**
     * Test maxNumberOfHistoryEntriesToGet
     */
    @Test
    public void testGetLocationDeploymentHistoryMaxEntryCount() {
        // create deployment history for a location in ddb table
        MitigationInstanceItemCreator itemCreator = getItemCreator(deviceName, serviceName, mitigationName, location,
                mitigationStatus, blockingDeviceWorkflowId, schedulingStatus);

        // add deployment history
        int deploymentCount = 10;
        DateTime dateTime = DateTime.now();
        for (int v = 1; v <= deploymentCount; ++v) {
            itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, v + 10000));
            itemCreator.setCreateDate(dateTime.plusMinutes(v).toString(CREATE_DATE_FORMATTER));
            itemCreator.setMitigationVersion(v);
            itemCreator.addItem();
        }

        int maxEntryCount = 5;
        List<LocationDeploymentInfo> instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName,
                serviceName, location, maxEntryCount, null, tsdMetrics);

        assertEquals(maxEntryCount, instanceStatuses.size());
        for (int i = 0; i < maxEntryCount; ++i) {
            assertEquals(deploymentCount - i, instanceStatuses.get(i).getMitigationVersion());
        }
    }

    /**
     * Test start of deployment create time
     */
    @Test
    public void testGetLocationDeploymentHistoryDeploymentCreateTime() {
        // create deployment history for a location in ddb table
        MitigationInstanceItemCreator itemCreator = getItemCreator(deviceName, serviceName, mitigationName, location,
                mitigationStatus, blockingDeviceWorkflowId, schedulingStatus);

        // add deployment history
        int deploymentCount = 10;
        DateTime dateTime = DateTime.now();
        for (int v = 1; v <= deploymentCount; ++v) {
            itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, v + 10000));
            itemCreator.setCreateDate(dateTime.plusMinutes(v).toString(CREATE_DATE_FORMATTER));
            itemCreator.setMitigationVersion(v);
            itemCreator.addItem();
        }

        int maxEntryCount = 5;
        long deploymentTime = dateTime.plusMinutes(8).getMillis();
        List<LocationDeploymentInfo> instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName,
                serviceName, location, maxEntryCount, deploymentTime, tsdMetrics);

        assertEquals(maxEntryCount, instanceStatuses.size());
        for (int i = 0; i < maxEntryCount; ++i) {
            assertEquals(deploymentCount - i - 3, instanceStatuses.get(i).getMitigationVersion());
        }

        instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName, serviceName, location, 10,
                deploymentTime, tsdMetrics);

        assertEquals(7, instanceStatuses.size());
        for (int i = 0; i < 7; ++i) {
            assertEquals(deploymentCount - i - 3, instanceStatuses.get(i).getMitigationVersion());
        }
    }

    /**
     * Test filter device name
     */
    @Test
    public void testGetLocationDeploymentHistoryfilterDeviceName() {
        // create deployment history for a location in ddb table
        MitigationInstanceItemCreator itemCreator = getItemCreator(deviceName, serviceName, mitigationName, location,
                mitigationStatus, blockingDeviceWorkflowId, schedulingStatus);

        // add deployment history
        int deploymentCount = 10;
        DateTime dateTime = DateTime.now();
        for (int v = 1; v <= deploymentCount; ++v) {
            itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, v + 10000));
            itemCreator.setCreateDate(dateTime.plusMinutes(v).toString(CREATE_DATE_FORMATTER));
            itemCreator.setMitigationVersion(v);
            itemCreator.addItem();
        }

        itemCreator.setDeviceName("anotherDevice");
        itemCreator.setDeviceWorkflowId(getDeviceWorkflowId("anotherDevice", 100));
        itemCreator.setCreateDate(dateTime.plusMinutes(20).toString(CREATE_DATE_FORMATTER));
        itemCreator.setMitigationVersion(20);
        itemCreator.addItem();

        List<LocationDeploymentInfo> instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName,
                serviceName, location, 20, null, tsdMetrics);

        assertEquals(deploymentCount, instanceStatuses.size());
        for (int i = 0; i < deploymentCount; ++i) {
            assertEquals(deploymentCount - i, instanceStatuses.get(i).getMitigationVersion());
        }
    }

    /**
     * Test filter service name
     */
    @Test
    public void testGetLocationDeploymentHistoryfilterServiceName() {
        // create deployment history for a location in ddb table
        MitigationInstanceItemCreator itemCreator = getItemCreator(deviceName, serviceName, mitigationName, location,
                mitigationStatus, blockingDeviceWorkflowId, schedulingStatus);

        // add deployment history
        int deploymentCount = 10;
        DateTime dateTime = DateTime.now();
        for (int v = 1; v <= deploymentCount; ++v) {
            itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, v + 10000));
            itemCreator.setCreateDate(dateTime.plusMinutes(v).toString(CREATE_DATE_FORMATTER));
            itemCreator.setMitigationVersion(v);
            itemCreator.addItem();
        }

        itemCreator.setServiceName("otherService");
        itemCreator.setDeviceWorkflowId(getDeviceWorkflowId(deviceName, 20000));
        itemCreator.setCreateDate(dateTime.plusMinutes(20).toString(CREATE_DATE_FORMATTER));
        itemCreator.setMitigationVersion(20);
        itemCreator.addItem();

        List<LocationDeploymentInfo> instanceStatuses = mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName,
                serviceName, location, 20, null, tsdMetrics);

        assertEquals(deploymentCount, instanceStatuses.size());
        for (int i = 0; i < deploymentCount; ++i) {
            assertEquals(deploymentCount - i, instanceStatuses.get(i).getMitigationVersion());
        }
    }
    
    /**
     * Test location does not have any deployment history
     */
    @Test(expected = MissingLocationException400.class)
    public void testGetLocationDeploymentHistoryAtNonExistingLocation() {
        mitigationInfoHandler.getLocationDeploymentInfoOnLocation(deviceName, serviceName, location, 20, null, tsdMetrics);
    }
}
