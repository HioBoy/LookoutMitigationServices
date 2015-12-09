package com.amazon.lookout.mitigation.service.workflow;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.StartWorkflowOptions;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.amazon.lookout.workflow.LookoutMitigationWorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowType;

import org.mockito.ArgumentCaptor;

import java.util.HashSet;

public class SWFWorkflowStarterImplTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    @Test
    public void testRunIdReturnedOnCreatingNewWorkflowClient() {
        LookoutMitigationWorkflowClientExternal mockWorkflowClient = mock(LookoutMitigationWorkflowClientExternal.class);
        SWFWorkflowClientProvider mockWorkflowClientProvider = mock(SWFWorkflowClientProvider.class);
        when(mockWorkflowClientProvider.getMitigationModificationWorkflowClient(anyString(), anyString(), anyLong())).thenReturn(mockWorkflowClient);
        
        MitigationModificationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
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

    @Test
    public void testWorkflowExecutionStartToCloseTimeoutForRouterMitigations() {
        testWorkflowExecutionStartToCloseTimeout(
                DeviceName.POP_ROUTER,
                SWFWorkflowStarterImpl.DEFAULT_WORKFLOW_COMPLETION_TIMEOUT_SECONDS);
    }

    @Test
    public void testWorkflowExecutionStartToCloseTimeoutForIPTablesMitigations() {
        testWorkflowExecutionStartToCloseTimeout(
                DeviceName.POP_HOSTS_IP_TABLES,
                SWFWorkflowStarterImpl.IPTABLES_WORKFLOW_COMPLETION_TIMEOUT_SECONDS);
    }

    private void testWorkflowExecutionStartToCloseTimeout(DeviceName deviceName, long expectedTimeout) {
        LookoutMitigationWorkflowClientExternal workflowClientSpy = workflowClientStub();
        SWFWorkflowStarterImpl workflowStarterImpl = new SWFWorkflowStarterImpl(
                workflowClientProviderReturning(workflowClientSpy));

        workflowStarterImpl.startMitigationModificationWorkflow(
                anyWorkflowId(),
                anyRequest(),
                anyLocations(),
                RequestType.CreateRequest,
                anyMitigationVersion(),
                deviceName.name(),
                DeviceScope.GLOBAL.name(),
                workflowClientSpy,
                TestUtils.newNopTsdMetrics());

        ArgumentCaptor<StartWorkflowOptions> startWorkflowOptionsCaptor =
                ArgumentCaptor.forClass(StartWorkflowOptions.class);
        verify(workflowClientSpy).startMitigationWorkflow(
                anyInt(),
                any(),
                any(),
                any(),
                anyInt(),
                anyString(),
                anyString(),
                startWorkflowOptionsCaptor.capture());
        assertThat(
                startWorkflowOptionsCaptor.getValue().getExecutionStartToCloseTimeoutSeconds(),
                is(expectedTimeout));
    }

    private SWFWorkflowClientProvider workflowClientProviderReturning(
            LookoutMitigationWorkflowClientExternal workflowClient) {

        SWFWorkflowClientProvider workflowClientProviderStub = mock(SWFWorkflowClientProvider.class);
        when(workflowClientProviderStub.getMitigationModificationWorkflowClient(anyString(), anyString(), anyLong()))
                .thenReturn(workflowClient);
        return workflowClientProviderStub;
    }

    private MitigationModificationRequest anyRequest() {
        return RequestTestHelper.generateCreateMitigationRequest();
    }

    private LookoutMitigationWorkflowClientExternal workflowClientStub() {
        LookoutMitigationWorkflowClientExternal result = mock(LookoutMitigationWorkflowClientExternal.class);
        WorkflowExecution workflowExecution = new WorkflowExecution()
                .withRunId("TestRunId")
                .withWorkflowId("TestWorkflowId");
        WorkflowType workflowType = new WorkflowType()
                .withName("TestWorkflowName")
                .withVersion("TestVersion");
        when(result.getWorkflowExecution()).thenReturn(workflowExecution);
        when(result.getWorkflowType()).thenReturn(workflowType);
        return result;
    }

    private int anyMitigationVersion() {
        return 1;
    }

    private int anyWorkflowId() {
        return 1;
    }

    private HashSet<String> anyLocations() {
        return newHashSet("LocationA");
    }
}
