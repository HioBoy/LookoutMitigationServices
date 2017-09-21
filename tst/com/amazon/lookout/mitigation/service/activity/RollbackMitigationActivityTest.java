package com.amazon.lookout.mitigation.service.activity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Matchers.any;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;

import com.amazon.lookout.mitigation.RequestCreator;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.WorkflowIdsDAO;
import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

public class RollbackMitigationActivityTest extends ActivityTestHelper {
    private static RollbackMitigationActivity rollbackMitigationActivity;
    
    private RequestStorageManager requestStorageManager;
    private SWFWorkflowStarter swfWorkflowStarter;
    private RollbackMitigationRequest request;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator(mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class)));
        requestStorageManager = mock(RequestStorageManager.class);
        swfWorkflowStarter = mock(SWFWorkflowStarter.class);
        rollbackMitigationActivity = new RollbackMitigationActivity(requestValidator,
                requestStorageManager,
                swfWorkflowStarter,
                requestInfoHandler,
                mock(TemplateBasedRequestValidator.class),
                mock(CurrentRequestsDAO.class),
                mock(ArchivedRequestsDAO.class),
                mock(RequestCreator.class),
                mock(SwitcherooDAO.class));
        
        request = new RollbackMitigationRequest();
        request.setServiceName(serviceName);
        request.setMitigationName(mitigationName);
        request.setRollbackToMitigationVersion(rollbackMitigationVersion);
        request.setMitigationVersion(mitigationVersion);
        request.setMitigationTemplate(mitigationTemplate);
        request.setMitigationActionMetadata(mitigationActionMetadata);
        request.setDeviceName(DeviceName.BLACKWATCH_BORDER.name());
        request.setLocation(locations.get(0));
    }
    
    /**
     * Test invalid input null
     */
    @Test(expected = NullPointerException.class)
    public void testInvalidInput() {
        rollbackMitigationActivity.enact(null);
    }
    
    /**
     * Test invalid input, invalid mitigation name
     */
    @Test(expected = BadRequest400.class)
    public void testMissingParameters2() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        request.setMitigationName("invalid_mitigation_name\000");
        request.setRollbackToMitigationVersion(1000);
        
        rollbackMitigationActivity.enact(request);
    }
  
    /**
     * Test invalid input, missing rollback mitigation version
     */
    @Test(expected = BadRequest400.class)
    public void testMissingParameters3() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setRollbackToMitigationVersion(0);
        
        rollbackMitigationActivity.enact(request);
    }
  
    /**
     * Test rollback version's template does not match the one in request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidMitigationTemplate() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        mitigationRequestDescription.setMitigationTemplate("differentTemplate");
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
   
    /**
     * Test rollback to delete request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequestType() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        mitigationRequestDescription.setMitigationTemplate(mitigationTemplate);
        mitigationRequestDescription.setRequestType(RequestType.DeleteRequest.toString());
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
    
    /**
     * Test missing mitigaiton case
     */
    @Test(expected = MissingMitigationVersionException404.class)
    public void testMissingMitigation() {
        doThrow(new MissingMitigationVersionException404()).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
    
    /**
     * Test failed to persist request to request table, and throw IllegalArgumentException
     */
    @Test(expected = BadRequest400.class)
    public void failedStoreRequestToTable1() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        request.setServiceName(serviceName);
        request.setMitigationName(mitigationName);
        request.setRollbackToMitigationVersion(mitigationVersion);
        
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(mitigationVersion), isA(TSDMetrics.class));
        
        doThrow(new IllegalArgumentException()).when(requestStorageManager).storeRequestForWorkflow(
                eq(request), eq(new HashSet<>(locations)), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
     
    /**
     * Test failed to persist request to request table, and throw StaleRequestException400
     */
    @Test(expected = StaleRequestException400.class)
    public void failedStoreRequestToTable2() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        
        doNothing().when(requestValidator).validateRollbackRequest(request, mitigationRequestDescription);
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        doThrow(new StaleRequestException400()).when(requestStorageManager).storeRequestForWorkflow(
                any(), eq(new HashSet<>(locations)), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
    
    /**
     * Test failed to persist request to request table, and throw runtimeException
     */
    @Test(expected = InternalServerError500.class)
    public void failedStoreRequestToTable3() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        
        doNothing().when(requestValidator).validateRollbackRequest(request, mitigationRequestDescription);
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        doThrow(new RuntimeException()).when(requestStorageManager).storeRequestForWorkflow(eq(request),
                eq(new HashSet<>(locations)), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
     }
    
    /**
     * throw IllegalStateException during startWorkflow
     */
    @Test(expected = BadRequest400.class)
    public void failedStartWorkflow() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        doNothing().when(requestValidator).validateRollbackRequest(request, mitigationRequestDescription);
        
        doReturn(requestStorageResponse).when(requestStorageManager).storeRequestForWorkflow(any(),
                eq(new HashSet<>(locations)), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
        
        WorkflowClientExternal workflowClient = mock(WorkflowClientExternal.class);
        
        doReturn(workflowClient).when(swfWorkflowStarter).createMitigationModificationWorkflowClient(
                eq(workflowId), any(), eq(deviceName), isA(TSDMetrics.class));
        
        doThrow(new IllegalStateException()).when(swfWorkflowStarter)
                .startMitigationModificationWorkflow(eq(workflowId), any(), eq(new HashSet<>(locations)),
                        eq(RequestType.RollbackRequest), eq(mitigationVersion), eq(deviceName),
                        eq(workflowClient), isA(TSDMetrics.class));
        
        rollbackMitigationActivity.enact(request);
    }
    
    /**
     * successfully process rollback request
     */
    @Test
    public void successfullyProcessRequest() {
        MitigationRequestDescriptionWithLocations originalModificationRequest =
                new MitigationRequestDescriptionWithLocations();
        originalModificationRequest.setLocations(locations);
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        mitigationRequestDescription.setMitigationTemplate(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer);
        mitigationRequestDescription.setServiceName(ServiceName.AWS);
        mitigationRequestDescription.setDeviceName(DeviceName.BLACKWATCH_BORDER.name());
        mitigationRequestDescription.setMitigationName(mitigationName);
        originalModificationRequest.setMitigationRequestDescription(mitigationRequestDescription);
        
        doReturn(originalModificationRequest).when(requestInfoHandler)
                .getMitigationDefinition(eq(deviceName), eq(serviceName), eq(mitigationName),
                        eq(rollbackMitigationVersion), isA(TSDMetrics.class));
        
        doReturn(requestStorageResponse).when(requestStorageManager).storeRequestForWorkflow(any(),
                eq(new HashSet<>(locations)), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
 
        WorkflowClientExternal workflowClient = mock(WorkflowClientExternal.class);
        
        doReturn(workflowClient).when(swfWorkflowStarter).createMitigationModificationWorkflowClient(
                eq(workflowId), any(), eq(deviceName), isA(TSDMetrics.class));
        
        doNothing().when(swfWorkflowStarter).startMitigationModificationWorkflow(
                eq(workflowId), any(), eq(new HashSet<>(locations)),
                eq(RequestType.RollbackRequest), eq(mitigationVersion), eq(deviceName),
                eq(workflowClient), isA(TSDMetrics.class));
        
        String swfRunId = "runid";
        WorkflowExecution workflowExecution = mock(WorkflowExecution.class);
        doReturn(workflowExecution).when(workflowClient).getWorkflowExecution();
        doReturn(swfRunId).when(workflowExecution).getRunId();
        
        doNothing().when(requestStorageManager).updateRunIdForWorkflowRequest(
                eq(deviceName), eq(workflowId), eq(swfRunId), eq(RequestType.RollbackRequest), isA(TSDMetrics.class));
        
        MitigationModificationResponse response = rollbackMitigationActivity.enact(request);
        assertEquals(mitigationName, response.getMitigationName());
        assertEquals(mitigationVersion, response.getMitigationVersion());
        assertEquals(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer, response.getMitigationTemplate());
        assertEquals(deviceName, response.getDeviceName());
        assertEquals(serviceName, response.getServiceName());
        assertEquals(workflowId, response.getJobId());
        assertEquals(WorkflowStatus.RUNNING, response.getRequestStatus());
        
        List<String> responseLocations = new ArrayList<>();
        
        for (MitigationInstanceStatus instanceStatus : response.getMitigationInstanceStatuses()) {
            assertEquals(MitigationStatus.CREATED, instanceStatus.getMitigationStatus());
            responseLocations.add(instanceStatus.getLocation());
        }

        assertEquals(locations, responseLocations);
    }
}
