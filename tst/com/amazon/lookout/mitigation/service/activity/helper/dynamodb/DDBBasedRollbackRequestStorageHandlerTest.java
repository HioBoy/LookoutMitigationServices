package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.MitigationRequestItemCreator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.request.RollbackMitigationRequestInternal;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.util.Tables;

import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.*;
import static com.amazon.lookout.ddb.model.MitigationRequestsModel.*;

public class DDBBasedRollbackRequestStorageHandlerTest {

    private static final String domain = "unit-test";
    
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static DDBBasedRollbackRequestStorageHandler ddbBasedRollbackRequestStorageHandler;
    private static RequestTableTestHelper requestTableTestHelper;
    private static String tableName = MitigationRequestsModel.getInstance().getTableName(domain);
    
    private final TSDMetrics metrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
    }
    
    @Before
    public void setUpBeforeTest() throws InterruptedException {
        when(metrics.newSubMetrics(anyString())).thenReturn(metrics);
        CreateTableRequest request = MitigationRequestsModel.getInstance().getCreateTableRequest(tableName);
        if (Tables.doesTableExist(dynamoDBClient, tableName)) {
            dynamoDBClient.deleteTable(tableName);
        }
        dynamoDBClient.createTable(request);
        Tables.awaitTableToBecomeActive(dynamoDBClient, tableName);
        ddbBasedRollbackRequestStorageHandler = new DDBBasedRollbackRequestStorageHandler(dynamoDBClient, domain, mock(TemplateBasedRequestValidator.class));
        requestTableTestHelper = new RequestTableTestHelper(dynamodb, domain);
    }
     
    /**
     * Test storeRequestForWorkflow, with incorrect type of request
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParam1() {
        ddbBasedRollbackRequestStorageHandler.storeRequestForWorkflow(new EditMitigationRequest(), locations, metrics);
    }

    /**
     * Test storeRequestForWorkflow, with empty location
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParam2() {
        ddbBasedRollbackRequestStorageHandler.storeRequestForWorkflow(new RollbackMitigationRequestInternal(), null, metrics);
    }

    /**
     * Test storeRequestForWorkflow, with null param
     */
    @Test(expected = NullPointerException.class)
    public void testInvalidParam3() {
        ddbBasedRollbackRequestStorageHandler.storeRequestForWorkflow(null, locations, metrics);
    }

    /**
     * Test storeRequestForWorkflow, with mitigation does not exist
     */
    @Test(expected = NullPointerException.class)
    public void testInvalidParam4() {
        RollbackMitigationRequestInternal request = new RollbackMitigationRequestInternal();
        request.setDeviceName(deviceName);
        request.setDeviceScope(deviceScope);
        request.setMitigationName(mitigationName);
        
        ddbBasedRollbackRequestStorageHandler.storeRequestForWorkflow(request, locations, metrics);
    }

    /**
     * Test successfully store the request
     */
    @Test
    public void testSuccessStore() {
        RollbackMitigationRequestInternal request = new RollbackMitigationRequestInternal();
        request.setDeviceName(deviceName);
        request.setDeviceScope(deviceScope);
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer);
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setDescription("desc");
        mitigationActionMetadata.setRelatedTickets(Arrays.asList("ticket"));
        mitigationActionMetadata.setToolName("test tool");
        mitigationActionMetadata.setUser("user");
        request.setMitigationActionMetadata(mitigationActionMetadata);
        request.setServiceName(serviceName);
        request.setMitigationDefinition(mitigationDefinition);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());
        
        int maxMitigationVersion = 10;
        int maxWorkflowId = 20;
        
        request.setMitigationVersion(maxMitigationVersion + 1);
        MitigationRequestItemCreator itemCreator = requestTableTestHelper.getItemCreator(
                deviceName, serviceName, mitigationName, deviceScope);
        itemCreator.setMitigationVersion(maxMitigationVersion);
        itemCreator.setWorkflowId(maxWorkflowId - 1);
        itemCreator.addItem();
        
        itemCreator.setMitigationName("OtherMitigation");
        itemCreator.setMitigationVersion(maxMitigationVersion);
        itemCreator.setWorkflowId(maxWorkflowId);
        itemCreator.addItem();
        
        assertEquals(maxWorkflowId + 1, ddbBasedRollbackRequestStorageHandler
                .storeRequestForWorkflow(request, locations, metrics));
        assertEquals(maxMitigationVersion + 1, dynamodb.getTable(tableName)
                .getItem(DEVICE_NAME_KEY, deviceName, WORKFLOW_ID_KEY, maxWorkflowId + 1)
                .getInt(MITIGATION_VERSION_KEY));
    }
}
