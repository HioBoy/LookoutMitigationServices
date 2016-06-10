package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DDBBasedRequestStorageManagerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    /**
     * Test the case where we pass in a valid request to the StorageManager and the manager is able to store the request. 
     */
    @Test
    public void testHappyCase() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        
        DDBBasedCreateRequestStorageHandler createRequestStorageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        when(storageManager.getRequestStorageHandler(RequestType.CreateRequest)).thenReturn(createRequestStorageHandler);
        
        long workflowIdToReturn = 1;
        when(createRequestStorageHandler.storeRequestForWorkflow(any(MitigationModificationRequest.class), anySet(),
                any(TSDMetrics.class))).thenReturn(new RequestStorageResponse(workflowIdToReturn, 1));
        
        when(storageManager.storeRequestForWorkflow(any(MitigationModificationRequest.class), anySet(), any(RequestType.class), any(TSDMetrics.class))).thenCallRealMethod();
        
        CreateMitigationRequest request = new CreateMitigationRequest();
        long workflowId = storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), RequestType.CreateRequest, tsdMetrics).getWorkflowId();
        assertEquals(workflowId, workflowIdToReturn);
    }
    
    /**
     * Test the case where there is no storage handler for the type of request being stored.
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testNoStorageHandlerCase() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        CreateMitigationRequest request = new CreateMitigationRequest();
        RequestType requestType = RequestType.CreateRequest;
        when(storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics)).thenCallRealMethod();
        when(storageManager.getRequestStorageHandler(any(RequestType.class))).thenReturn(null);
        
        Throwable caughtException = null;
        try {
            storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics).getWorkflowId();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the storage handler throws an exception when storing this request.
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testStorageHandlerExceptionCase() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        
        CreateMitigationRequest request = new CreateMitigationRequest();
        RequestType requestType = RequestType.DeleteRequest;
        
        DDBBasedDeleteRequestStorageHandler deleteRequestStorageHandler = mock(DDBBasedDeleteRequestStorageHandler.class);
        when(deleteRequestStorageHandler.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), tsdMetrics)).thenThrow(new RuntimeException());
        when(storageManager.getRequestStorageHandler(requestType)).thenReturn(deleteRequestStorageHandler);
        
        when(storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics)).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics).getWorkflowId();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the happy case we successfully update the abort flag. 
     */
    @Test
    public void testrequestAbortForWorkflowRequestSucceed() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        
        DDBBasedRequestStorageHandler baseRequestStorageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        when(storageManager.getBaseRequestStorageHandler()).thenReturn(baseRequestStorageHandler);
        Mockito.doNothing().when(baseRequestStorageHandler).updateAbortFlagForWorkflowRequest(anyString(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        
        Mockito.doCallRealMethod().when(storageManager).requestAbortForWorkflowRequest(anyString(), anyLong(), any(TSDMetrics.class));

        storageManager.requestAbortForWorkflowRequest("device", 1, tsdMetrics);
    }
    
    /**
     * Test the case updateAbortFlagForWorkflowRequest throws exception. 
     */
    @Test
    public void testrequestAbortForWorkflowRequestException() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        
        DDBBasedRequestStorageHandler baseRequestStorageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        when(storageManager.getBaseRequestStorageHandler()).thenReturn(baseRequestStorageHandler);
        Mockito.doThrow(new RuntimeException()).when(baseRequestStorageHandler).updateAbortFlagForWorkflowRequest(anyString(), anyLong(), anyBoolean(), any(TSDMetrics.class));

        Mockito.doCallRealMethod().when(storageManager).requestAbortForWorkflowRequest(anyString(), anyLong(), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
        	storageManager.requestAbortForWorkflowRequest("device", 1, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
}
