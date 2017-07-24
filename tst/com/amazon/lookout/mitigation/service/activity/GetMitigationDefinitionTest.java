package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junitparams.JUnitParamsRunner;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.Parameters;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionResponse;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

@RunWith(JUnitParamsRunner.class)
public class GetMitigationDefinitionTest extends ActivityTestHelper {
    private static GetMitigationDefinitionActivity getMitigationDefinitionActivity;
    private static final GetMitigationDefinitionRequest request = new GetMitigationDefinitionRequest();
    static {
        request.setDeviceName(deviceName);
        request.setMitigationName(mitigationName);
        request.setMitigationVersion(mitigationVersion);
        request.setServiceName(serviceName);
    }
    
    @Before
    public void setupMore() {
        getMitigationDefinitionActivity = spy(new GetMitigationDefinitionActivity(
                requestValidator, requestInfoHandler, mitigationInstanceInfoHandler,
                mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class),
                mock(SwitcherooDAO.class)));
    }
    
    /**
     * Test validation failure
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidInput() {
        doThrow(new IllegalArgumentException()).when(requestValidator).validateGetMitigationDefinitionRequest(request);
        getMitigationDefinitionActivity.enact(request);
    }
    
    /**
     * Test missing mitigation version
     */
    @Test(expected = MissingMitigationVersionException404.class)
    public void missingMitigationVersion() {
        doThrow(new MissingMitigationVersionException404()).when(requestInfoHandler).getMitigationDefinition(
                eq(deviceName), eq(serviceName), eq(mitigationName), eq(mitigationVersion), isA(TSDMetrics.class));
        
        getMitigationDefinitionActivity.enact(request);
    }
    
    public Object[] instanceProvider() {
        return new Object[] {
                new ArrayList<MitigationInstanceStatus>(),
                Arrays.asList(new MitigationInstanceStatus(), new MitigationInstanceStatus())
        };
    } 
    
    /**
     * Test find request, with 0 or non-zero instance
     */
    @Test
    @Parameters(method="instanceProvider")
    public void successfullyFindRequest(List<MitigationInstanceStatus> instances) {
        MitigationRequestDescriptionWithLocations mitigationDescriptionWithLocations = 
                new MitigationRequestDescriptionWithLocations();
        
        MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
        mitigationRequestDescription.setJobId(workflowId);
        mitigationDescriptionWithLocations.setMitigationRequestDescription(mitigationRequestDescription);
        mitigationDescriptionWithLocations.setLocations(locations);
        
        doReturn(mitigationDescriptionWithLocations).when(requestInfoHandler).getMitigationDefinition(
                eq(deviceName), eq(serviceName), eq(mitigationName), eq(mitigationVersion), isA(TSDMetrics.class));
        
        doReturn(instances).when(mitigationInstanceInfoHandler)
                .getMitigationInstanceStatus(eq(deviceName), eq(workflowId), isA(TSDMetrics.class));
        
        GetMitigationDefinitionResponse response = getMitigationDefinitionActivity.enact(request);
        assertEquals(mitigationDescriptionWithLocations, response.getMitigationRequestDescriptionWithLocationAndStatus()
                .getMitigationRequestDescriptionWithLocations());
        assertEquals(instances, response.getMitigationRequestDescriptionWithLocationAndStatus().getInstancesStatus());
    }
}
