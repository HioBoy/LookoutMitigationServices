package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.activities.model.RequestType;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

public class DDBBasedRequestStorageManagerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
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
        when(createRequestStorageHandler.storeRequestForWorkflow(any(MitigationModificationRequest.class), any(TSDMetrics.class))).thenReturn(workflowIdToReturn);
        
        when(storageManager.storeRequestForWorkflow(any(MitigationModificationRequest.class), any(RequestType.class), any(TSDMetrics.class))).thenCallRealMethod();
        
        MitigationModificationRequest request = new MitigationModificationRequest();
        long workflowId = storageManager.storeRequestForWorkflow(request, RequestType.CreateRequest, tsdMetrics);
        assertEquals(workflowId, workflowIdToReturn);
    }
    
    /**
     * Test the case where there is no storage handler for the type of request being stored.
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testNoStorageHandlerCase() {
        DDBBasedRequestStorageManager storageManager = mock(DDBBasedRequestStorageManager.class);
        MitigationModificationRequest request = new MitigationModificationRequest();
        RequestType requestType = RequestType.CreateRequest;
        when(storageManager.storeRequestForWorkflow(request, requestType, tsdMetrics)).thenCallRealMethod();
        when(storageManager.getRequestStorageHandler(any(RequestType.class))).thenReturn(null);
        
        Throwable caughtException = null;
        try {
            storageManager.storeRequestForWorkflow(request, requestType, tsdMetrics);
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
        
        MitigationModificationRequest request = new MitigationModificationRequest();
        RequestType requestType = RequestType.CreateRequest;
        
        DDBBasedCreateRequestStorageHandler createRequestStorageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        when(createRequestStorageHandler.storeRequestForWorkflow(request, tsdMetrics)).thenThrow(new RuntimeException());
        when(storageManager.getRequestStorageHandler(requestType)).thenReturn(createRequestStorageHandler);
        
        when(storageManager.storeRequestForWorkflow(request, requestType, tsdMetrics)).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageManager.storeRequestForWorkflow(request, requestType, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
}
