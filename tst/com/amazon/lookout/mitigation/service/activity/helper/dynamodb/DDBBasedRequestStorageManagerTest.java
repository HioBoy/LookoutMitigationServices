package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.model.RequestType;

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
        when(createRequestStorageHandler.storeRequestForWorkflow(any(MitigationModificationRequest.class), anySet(), any(TSDMetrics.class))).thenReturn(workflowIdToReturn);
        
        when(storageManager.storeRequestForWorkflow(any(MitigationModificationRequest.class), anySet(), any(RequestType.class), any(TSDMetrics.class))).thenCallRealMethod();
        
        CreateMitigationRequest request = new CreateMitigationRequest();
        long workflowId = storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), RequestType.CreateRequest, tsdMetrics);
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
            storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics);
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
            storageManager.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), requestType, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
}
