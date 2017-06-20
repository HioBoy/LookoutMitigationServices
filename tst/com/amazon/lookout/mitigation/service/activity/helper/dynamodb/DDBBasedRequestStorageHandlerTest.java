package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;

import junitparams.JUnitParamsRunner;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.ImmutableSet;

@RunWith(JUnitParamsRunner.class)
public class DDBBasedRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private static final String domain = "unit-test";
    
    private static AmazonDynamoDBClient localDynamoDBClient;
    private static RequestTableTestHelper testTableHelper;
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging(Level.WARN);
        localDynamoDBClient = DynamoDBTestUtil.get().getClient();
    }
    
    @Before
    public void setUpBeforeTest() throws InterruptedException {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        DynamoDB dynamoDB = new DynamoDB(localDynamoDBClient);
        
        try {
            Table table = dynamoDB.getTable(MitigationRequestsModel.getInstance().getTableName(domain));
            table.delete();
            table.waitForDelete();
        } catch (ResourceNotFoundException e) {
            // Already deleted
        }
        MitigationRequestsModel.getInstance().createTableIfNotExist(localDynamoDBClient, domain);
        testTableHelper = new RequestTableTestHelper(new DynamoDB(localDynamoDBClient), domain);
    }

    private static MitigationRequestDescriptionWithLocations validateRequestInDDB(
            CreateMitigationRequest request, Set<String> locations, long workflowId) 
    {
        return testTableHelper.validateRequestInDDB(
                request, request.getMitigationDefinition(), DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION, 
                locations, workflowId);
    }
    
    private static void validateAbortFlagInRequestInDDB(String deviceName, long workflowId, boolean abortFlag) 
    {
    	assertEquals(testTableHelper.getRequestAbortFlag(deviceName, workflowId), abortFlag);
    }
    
    @Test
    public void testupdateAbortFlagForWorkflowRequest() {
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedRequestStorageHandler storageHandler =
                new DDBBasedRequestStorageHandler(spyClient, domain);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = 1;
        //store a request in DDB
        DeviceName deviceName = MitigationTemplateToDeviceMapper.getDeviceNameForTemplate(request.getMitigationTemplate());
        storageHandler.storeRequestInDDB(request, request.getMitigationDefinition(), locations, deviceName, workflowId, RequestType.CreateRequest, DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION, tsdMetrics);
       
        //first validate the request is successfully stored in DDB. 
        validateRequestInDDB(request, locations, workflowId);
        
        //set the abortflag to be true;
        storageHandler.updateAbortFlagForWorkflowRequest(deviceName.name(), workflowId, true, tsdMetrics);
        //validate the abortflag is set correctly in the request
        validateAbortFlagInRequestInDDB(deviceName.name(), workflowId, true);
        
        //set the abortflag to be false;
        storageHandler.updateAbortFlagForWorkflowRequest(deviceName.name(), workflowId, false, tsdMetrics);
        //validate the abortflag is set correctly in the request
        validateAbortFlagInRequestInDDB(deviceName.name(), workflowId, false);
    }
}
