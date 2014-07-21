package com.amazon.lookout.mitigation.service.workflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

public class SWFWorkflowStarterImplTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testRunIdReturnedOnCreatingNewWorkflowClient() {
        LookoutMitigationWorkflowClientExternal mockWorkflowClient = mock(LookoutMitigationWorkflowClientExternal.class);
        SWFWorkflowClientProvider mockWorkflowClientProvider = mock(SWFWorkflowClientProvider.class);
        when(mockWorkflowClientProvider.getMitigationModificationWorkflowClient(anyString(), anyString(), anyLong())).thenReturn(mockWorkflowClient);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateMitigationRequest();
        
        WorkflowExecution workflowExecution = new WorkflowExecution().withRunId("TestRunId").withWorkflowId("TestWorkflowId");
        WorkflowType workflowType = new WorkflowType().withName("TestWorkflowName").withVersion("TestVersion");
        when(mockWorkflowClient.getWorkflowExecution()).thenReturn(workflowExecution);
        when(mockWorkflowClient.getWorkflowType()).thenReturn(workflowType);
        
        SWFWorkflowStarterImpl workflowStarterImpl = new SWFWorkflowStarterImpl(mockWorkflowClientProvider);
        WorkflowClientExternal workflowClient = workflowStarterImpl.createMitigationModificationWorkflowClient(1, request, "TestDevice", TestUtils.newNopTsdMetrics());
        String swfRunId = workflowClient.getWorkflowExecution().getRunId();
        assertNotNull(swfRunId);
        assertEquals(swfRunId, "TestRunId");
    }
    
}
