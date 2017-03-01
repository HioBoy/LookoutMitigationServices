package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.InstanceTableTestHelper.deviceName;
import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.InstanceTableTestHelper.serviceName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.activities.model.MitigationInstanceSchedulingStatus;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.InstanceTableTestHelper.MitigationInstanceItemCreator;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

public class DDBBasedMitigationInstanceHandlerTest {
    private static final String domain = "unit-test";
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static InstanceTableTestHelper instanceTableTestHelper;
    private static DDBBasedMitigationInstanceHandler mitigationInstanceHandler; 
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
        instanceTableTestHelper = new InstanceTableTestHelper(dynamodb, domain);
        mitigationInstanceHandler = new DDBBasedMitigationInstanceHandler(dynamoDBClient, domain);
    }
    
    @Before
    public void setUpBeforeTest() {        
        String instanceTableName = MitigationInstancesModel.getInstance().getTableName(domain);
        CreateTableRequest request = MitigationInstancesModel.getInstance().getCreateTableRequest(instanceTableName);
        TableUtils.deleteTableIfExists(dynamoDBClient, new DeleteTableRequest(instanceTableName));
        dynamoDBClient.createTable(request);
    }
    
    @Test
    public void testGetMitigationInstanceAttributes() {
        
        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId("1");
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        List<String> attributesToGet = Arrays.asList(DDBBasedMitigationInstanceHandler.BLOCKING_DEVICE_WORKFLOW_ID_KEY, DDBBasedMitigationInstanceHandler.SCHEDULING_STATUS_KEY);
        Map<String, AttributeValue> result = mitigationInstanceHandler.getMitigationInstanceAttributes(2, deviceName, "TST1",  attributesToGet);
        assertEquals(result.size(), 2);
        assertEquals(result.get(DDBBasedMitigationInstanceHandler.BLOCKING_DEVICE_WORKFLOW_ID_KEY).getS(), "1");
        assertEquals(result.get(DDBBasedMitigationInstanceHandler.SCHEDULING_STATUS_KEY).getS(), SchedulingStatus.COMPLETED.name());
    }
    
    @Test
    public void testGetMitigationInstanceSchedulingStatus_InstanceNotExist() {
        
        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId("1");
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        MitigationInstanceSchedulingStatus result = mitigationInstanceHandler.getMitigationInstanceSchedulingStatus(3, deviceName, "TST1");
        assertNull(result);
    }
    
    @Test
    public void testGetMitigationInstanceSchedulingStatus_Succeed() {
        
        MitigationInstanceItemCreator instanceCreator = instanceTableTestHelper.getItemCreator(deviceName, serviceName);
        instanceCreator.setLocation("TST1");
        instanceCreator.setBlockingDeviceWorkflowId("test#1");
        instanceCreator.setWorkflowId("2");
        instanceCreator.addItem();
        MitigationInstanceSchedulingStatus result = mitigationInstanceHandler.getMitigationInstanceSchedulingStatus(2, deviceName, "TST1");
        assertEquals(result.getSchedulingStatus(), SchedulingStatus.COMPLETED);
        assertEquals(result.getBlockingInstanceInfo().getWorkflowId(), 1);
        assertEquals(result.getBlockingInstanceInfo().getDeviceName(), "test");
        assertNull(result.getBlockedInstanceInfo());

    }
}
