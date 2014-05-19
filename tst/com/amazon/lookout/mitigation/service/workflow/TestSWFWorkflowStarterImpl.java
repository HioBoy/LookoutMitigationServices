package com.amazon.lookout.mitigation.service.workflow;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;
import com.google.common.collect.Sets;

public class TestSWFWorkflowStarterImpl {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testRunIdReturnedOnCreatingNewWorkflowClient() {
        LookoutMitigationWorkflowClientExternal mockWorkflowClient = mock(LookoutMitigationWorkflowClientExternal.class);
        SWFWorkflowClientProvider mockWorkflowClientProvider = mock(SWFWorkflowClientProvider.class);
        when(mockWorkflowClientProvider.getWorkflowClient(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(mockWorkflowClient);
        
        TemplateBasedLocationsManager mockTemplateBasedLocationsHelper = mock(TemplateBasedLocationsManager.class);
        when(mockTemplateBasedLocationsHelper.getLocationsForDeployment(any(MitigationModificationRequest.class))).thenReturn(Sets.newHashSet("POP1", "POP2"));
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateMitigationRequest();
        
        WorkflowExecution workflowExecution = new WorkflowExecution().withRunId("TestRunId").withWorkflowId("TestWorkflowId");
        WorkflowType workflowType = new WorkflowType().withName("TestWorkflowName").withVersion("TestVersion");
        when(mockWorkflowClient.getWorkflowExecution()).thenReturn(workflowExecution);
        when(mockWorkflowClient.getWorkflowType()).thenReturn(workflowType);
        
        SWFWorkflowStarterImpl workflowStarterImpl = new SWFWorkflowStarterImpl(mockWorkflowClientProvider, mockTemplateBasedLocationsHelper);
        WorkflowClientExternal workflowClient = workflowStarterImpl.createSWFWorkflowClient(1, request, "TestDevice", TestUtils.newNopTsdMetrics());
        String swfRunId = workflowClient.getWorkflowExecution().getRunId();
        assertNotNull(swfRunId);
        assertEquals(swfRunId, "TestRunId");
    }
    
}
