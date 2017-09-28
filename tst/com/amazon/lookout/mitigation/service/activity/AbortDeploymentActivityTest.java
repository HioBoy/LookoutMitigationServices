package com.amazon.lookout.mitigation.service.activity;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.AbortDeploymentResponse;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

public class AbortDeploymentActivityTest extends ActivityTestHelper {
    private static AbortDeploymentActivity abortDeploymentActivity;
    
    private RequestStorageManager requestStorageManager;
    private AbortDeploymentRequest request;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator(
                mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class),
                "/random/path/location/json"));
        requestStorageManager = mock(RequestStorageManager.class);
        abortDeploymentActivity = new AbortDeploymentActivity(requestValidator, requestInfoHandler, requestStorageManager,
                mock(CurrentRequestsDAO.class), mock(SwitcherooDAO.class));
    }
    
    /**
     * Test invalid null request
     */
    @Test(expected = NullPointerException.class)
    public void testNullRequest() {
    	abortDeploymentActivity.enact(null);
    }
    
    /**
     * Test invalid request, invalid jobid
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidJobID() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(0);        
        abortDeploymentActivity.enact(request);
    }
    
    /**
     * Test invalid request, invalid service name
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidServiceName() {
        request = new AbortDeploymentRequest();
        request.setServiceName("InvalidServiceName");
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);    
        abortDeploymentActivity.enact(request);
    }
    
    /**
     * Test invalid request, invalid device name
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidDeviceName() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName("InvalidDeviceName");
        request.setJobId(workflowId);    
        abortDeploymentActivity.enact(request);
    }

    /**
     * Test invalid request, invalid mitigation template
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidMitigationTemplate() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate("InvalidTemplate");
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);    
        abortDeploymentActivity.enact(request);
    }
    
    /**
     * throw ConditionalCheckFailedException when update the abortflag in DDB
     */
    @Test(expected = InternalServerError500.class)
    public void testUpdateAbortFlagException() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);  
        MitigationNameAndRequestStatus mitigationNameAndRequestStatus = new MitigationNameAndRequestStatus(mitigationName, WorkflowStatus.RUNNING);
        
        doReturn(mitigationNameAndRequestStatus).when(requestInfoHandler).getMitigationNameAndRequestStatus(eq(deviceName), eq(mitigationTemplate), eq(workflowId), isA(TSDMetrics.class));
              
        doThrow(new ConditionalCheckFailedException("UpdateItem Conditional Check Fail")).when(requestStorageManager).requestAbortForWorkflowRequest(eq(deviceName), eq(workflowId), isA(TSDMetrics.class)); 
        abortDeploymentActivity.enact(request);
    }
    
    /**
     * test happy case, the abort deployment activities succeed.
     */
    @Test
    public void testAbortDeploymentActivitySucceed() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);  
        MitigationNameAndRequestStatus mitigationNameAndRequestStatus = new MitigationNameAndRequestStatus(mitigationName, WorkflowStatus.RUNNING);
        
        doReturn(mitigationNameAndRequestStatus).when(requestInfoHandler).getMitigationNameAndRequestStatus(eq(deviceName), eq(mitigationTemplate), eq(workflowId), isA(TSDMetrics.class));
              
        doNothing().when(requestStorageManager).requestAbortForWorkflowRequest(eq(deviceName), eq(workflowId), isA(TSDMetrics.class)); 
        AbortDeploymentResponse response = abortDeploymentActivity.enact(request);
        
        assertEquals(response.getMitigationName(), mitigationNameAndRequestStatus.getMitigationName());
        assertEquals(response.getServiceName(), serviceName);
        assertEquals(response.getDeviceName(), deviceName);
        assertEquals(response.getRequestStatus(), mitigationNameAndRequestStatus.getRequestStatus());
        verify(requestStorageManager, times(1)).requestAbortForWorkflowRequest(eq(deviceName), eq(workflowId), isA(TSDMetrics.class));
    }
    
    /**
     * test abort deployment activities fail when workflow already ends.
     */
    @Test
    public void testAbortDeploymentFail() {
        request = new AbortDeploymentRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);  
        MitigationNameAndRequestStatus mitigationNameAndRequestStatus = new MitigationNameAndRequestStatus(mitigationName, WorkflowStatus.SUCCEEDED);
        
        doReturn(mitigationNameAndRequestStatus).when(requestInfoHandler).getMitigationNameAndRequestStatus(eq(deviceName), eq(mitigationTemplate), eq(workflowId), isA(TSDMetrics.class));
              
        doNothing().when(requestStorageManager).requestAbortForWorkflowRequest(eq(deviceName), eq(workflowId), isA(TSDMetrics.class)); 
        AbortDeploymentResponse response = abortDeploymentActivity.enact(request);
        
        assertEquals(response.getMitigationName(), mitigationNameAndRequestStatus.getMitigationName());
        assertEquals(response.getServiceName(), serviceName);
        assertEquals(response.getDeviceName(), deviceName);
        assertEquals(response.getRequestStatus(), "WORKFLOWCOMPLETED_ABORTFAILED");
        verify(requestStorageManager, times(0)).requestAbortForWorkflowRequest(eq(deviceName), eq(workflowId), isA(TSDMetrics.class));
    }
}
