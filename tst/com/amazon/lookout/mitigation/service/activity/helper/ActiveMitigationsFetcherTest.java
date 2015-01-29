package com.amazon.lookout.mitigation.service.activity.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.model.RequestType;

public class ActiveMitigationsFetcherTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    /**
     * Test to ensure that the locations belonging to the same mitigation definition are consolidated into the same MitigationRequestDescriptionWithStatuses instance.
     */
    @Test
    public void testLocationsConsolidation() {
        String serviceName = ServiceName.Route53;
        String deviceName = DeviceName.POP_ROUTER.name();
        List<String> locations = null;
        
        TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        RequestInfoHandler requestInfoHandler = mock(RequestInfoHandler.class);
        
        List<ActiveMitigationDetails> activeMitigationDetails = new ArrayList<>();
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation1", 1, "TST1", deviceName, 1, new Date().getTime()));
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation3", 3, "TST3", deviceName, 1, new Date().getTime()));
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation1", 1, "TST2", deviceName, 1, new Date().getTime()));
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation2", 2, "TST1", deviceName, 2, new Date().getTime()));
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation2", 2, "TST2", deviceName, 2, new Date().getTime()));
        activeMitigationDetails.add(new ActiveMitigationDetails("Mitigation1", 1, "TST3", deviceName, 1, new Date().getTime()));
        
        when(activeMitigationInfoHandler.getActiveMitigationsForService(serviceName, deviceName, locations, tsdMetrics)).thenReturn(activeMitigationDetails);
        
        MitigationRequestDescription mitigation1Description = new MitigationRequestDescription();
        mitigation1Description.setMitigationName("Mitigation1");
        mitigation1Description.setJobId(1);
        mitigation1Description.setDeviceName(deviceName);
        mitigation1Description.setServiceName(serviceName);
        mitigation1Description.setRequestType(RequestType.CreateRequest.name());
        mitigation1Description.setMitigationVersion(1);
        when(requestInfoHandler.getMitigationRequestDescription(deviceName, 1, tsdMetrics)).thenReturn(mitigation1Description);
        
        MitigationRequestDescription mitigation2Description = new MitigationRequestDescription();
        mitigation2Description.setMitigationName("Mitigation2");
        mitigation2Description.setJobId(2);
        mitigation2Description.setDeviceName(deviceName);
        mitigation2Description.setServiceName(serviceName);
        mitigation2Description.setRequestType(RequestType.EditRequest.name());
        mitigation2Description.setMitigationVersion(2);
        when(requestInfoHandler.getMitigationRequestDescription(deviceName, 2, tsdMetrics)).thenReturn(mitigation2Description);
        
        MitigationRequestDescription mitigation3Description = new MitigationRequestDescription();
        mitigation3Description.setMitigationName("Mitigation3");
        mitigation3Description.setJobId(3);
        mitigation3Description.setDeviceName(deviceName);
        mitigation3Description.setServiceName(serviceName);
        mitigation3Description.setRequestType(RequestType.CreateRequest.name());
        mitigation3Description.setMitigationVersion(1);
        when(requestInfoHandler.getMitigationRequestDescription(deviceName, 3, tsdMetrics)).thenReturn(mitigation3Description);
        
        ActiveMitigationsFetcher fetcher = new ActiveMitigationsFetcher(serviceName, deviceName, locations, activeMitigationInfoHandler, requestInfoHandler, tsdMetrics);
        
        List<MitigationRequestDescriptionWithStatuses> response = fetcher.call();
        assertEquals(response.size(), 3);
        
        MitigationRequestDescriptionWithStatuses mitigation1Response = null;
        MitigationRequestDescriptionWithStatuses mitigation2Response = null;
        MitigationRequestDescriptionWithStatuses mitigation3Response = null;
        for (MitigationRequestDescriptionWithStatuses desc : response) {
            if (desc.getMitigationRequestDescription().getMitigationName().equals("Mitigation1")) {
                mitigation1Response = desc;
            }
            
            if (desc.getMitigationRequestDescription().getMitigationName().equals("Mitigation2")) {
                mitigation2Response = desc;
            }
            
            if (desc.getMitigationRequestDescription().getMitigationName().equals("Mitigation3")) {
                mitigation3Response = desc;
            }
        }
        
        assertNotNull(mitigation1Response);
        assertNotNull(mitigation2Response);
        assertNotNull(mitigation3Response);
        
        assertEquals(mitigation1Response.getInstancesStatusMap().size(), 3);
        assertTrue(mitigation1Response.getInstancesStatusMap().containsKey("TST1"));
        assertTrue(mitigation1Response.getInstancesStatusMap().get("TST1").getMitigationStatus().equals(MitigationStatus.DEPLOY_SUCCEEDED));
        
        assertTrue(mitigation1Response.getInstancesStatusMap().containsKey("TST2"));
        assertTrue(mitigation1Response.getInstancesStatusMap().get("TST2").getMitigationStatus().equals(MitigationStatus.DEPLOY_SUCCEEDED));
        
        assertTrue(mitigation1Response.getInstancesStatusMap().containsKey("TST3"));
        assertTrue(mitigation1Response.getInstancesStatusMap().get("TST3").getMitigationStatus().equals(MitigationStatus.DEPLOY_SUCCEEDED));
        
        assertEquals(mitigation2Response.getInstancesStatusMap().size(), 2);
        assertTrue(mitigation2Response.getInstancesStatusMap().containsKey("TST1"));
        assertTrue(mitigation2Response.getInstancesStatusMap().get("TST1").getMitigationStatus().equals(MitigationStatus.EDIT_SUCCEEDED));
        
        assertTrue(mitigation2Response.getInstancesStatusMap().containsKey("TST2"));
        assertTrue(mitigation2Response.getInstancesStatusMap().get("TST2").getMitigationStatus().equals(MitigationStatus.EDIT_SUCCEEDED));
        
        assertEquals(mitigation3Response.getInstancesStatusMap().size(), 1);
        assertTrue(mitigation3Response.getInstancesStatusMap().containsKey("TST3"));
        assertTrue(mitigation3Response.getInstancesStatusMap().get("TST3").getMitigationStatus().equals(MitigationStatus.DEPLOY_SUCCEEDED));
    }
    
}
