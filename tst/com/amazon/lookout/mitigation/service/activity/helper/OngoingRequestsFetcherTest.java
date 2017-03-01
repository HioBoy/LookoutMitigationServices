package com.amazon.lookout.mitigation.service.activity.helper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

public class OngoingRequestsFetcherTest {
    
    private static final List<String> LOCATIONS_1 = Arrays.asList("A", "B", "C");
    private static final List<String> LOCATIONS_2 = Arrays.asList("C", "D", "E");
    private static final MitigationRequestDescription REQUEST_DESCRIPTION_1 = new MitigationRequestDescription();
    private static final MitigationRequestDescription REQUEST_DESCRIPTION_2 = new MitigationRequestDescription();
    private static final String SERVICE_NAME = ServiceName.Route53;
    private static final String DEVICE_NAME = DeviceName.POP_ROUTER.name();
    private static final long JOB_ID_1 = 1l;
    private static final long JOB_ID_2 = 2l;
    private static final MitigationRequestDescriptionWithLocations REQUEST_1 = new MitigationRequestDescriptionWithLocations();
    private static final MitigationRequestDescriptionWithLocations REQUEST_2 = new MitigationRequestDescriptionWithLocations();
    private static final List<MitigationInstanceStatus> REQUEST_1_INSTANCES = new ArrayList<>();
    private static final List<MitigationInstanceStatus> REQUEST_2_INSTANCES = new ArrayList<>();
    
    static {
        REQUEST_DESCRIPTION_1.setDeviceName(DEVICE_NAME);
        REQUEST_DESCRIPTION_1.setJobId(JOB_ID_1);
        REQUEST_1.setLocations(LOCATIONS_1);
        REQUEST_1.setMitigationRequestDescription(REQUEST_DESCRIPTION_1);
        MitigationInstanceStatus instance = new MitigationInstanceStatus();
        instance.setLocation("A");
        instance.setMitigationStatus(MitigationStatus.BLOCKED);
        REQUEST_1_INSTANCES.add(instance);
        instance = new MitigationInstanceStatus();
        instance.setLocation("B");
        instance.setMitigationStatus(MitigationStatus.BLOCKED);
        REQUEST_1_INSTANCES.add(instance);
        instance = new MitigationInstanceStatus();
        instance.setLocation("C");
        instance.setMitigationStatus(MitigationStatus.BLOCKED);
        REQUEST_1_INSTANCES.add(instance);
        
        REQUEST_DESCRIPTION_2.setDeviceName(DEVICE_NAME);
        REQUEST_DESCRIPTION_2.setJobId(JOB_ID_2);
        REQUEST_2.setLocations(LOCATIONS_2);
        REQUEST_2.setMitigationRequestDescription(REQUEST_DESCRIPTION_2);
        instance = new MitigationInstanceStatus();
        instance.setLocation("D");
        instance.setMitigationStatus(MitigationStatus.BLOCKED);
        REQUEST_2_INSTANCES.add(instance);
    }
    
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    /**
     * Test request does not have location filter
     */
    @Test
    public void testNoLocationFilter() {
        TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        RequestInfoHandler requestInfoHandler = mock(RequestInfoHandler.class);
        
        List<MitigationRequestDescriptionWithLocations> ongoingMitigationRequests = new ArrayList<>();
        ongoingMitigationRequests.add(REQUEST_1);
        ongoingMitigationRequests.add(REQUEST_2);
        
        when(requestInfoHandler.getOngoingRequestsDescription(SERVICE_NAME, DEVICE_NAME, null, tsdMetrics)).thenReturn(ongoingMitigationRequests);
        
        MitigationInstanceInfoHandler instanceInfoHandler = mock(MitigationInstanceInfoHandler.class);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_1, tsdMetrics)).thenReturn(REQUEST_1_INSTANCES);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_2, tsdMetrics)).thenReturn(REQUEST_2_INSTANCES);
         
        OngoingRequestsFetcher ongoingRequestsFetcher = new OngoingRequestsFetcher(requestInfoHandler, SERVICE_NAME, DEVICE_NAME, null, instanceInfoHandler, tsdMetrics);
        
        List<MitigationRequestDescriptionWithStatuses> response = ongoingRequestsFetcher.call();
        // validate all locations status is returned
        // for missing location instance status, default RUNNING
        assertEquals(response.get(0).getInstancesStatusMap().get("A").getMitigationStatus(), MitigationStatus.BLOCKED);
        assertEquals(response.get(0).getInstancesStatusMap().get("B").getMitigationStatus(), MitigationStatus.BLOCKED);
        assertEquals(response.get(0).getInstancesStatusMap().get("C").getMitigationStatus(), MitigationStatus.BLOCKED);
        assertEquals(response.get(1).getInstancesStatusMap().get("C").getMitigationStatus(), MitigationStatus.RUNNING);
        assertEquals(response.get(1).getInstancesStatusMap().get("D").getMitigationStatus(), MitigationStatus.BLOCKED);
        assertEquals(response.get(1).getInstancesStatusMap().get("E").getMitigationStatus(), MitigationStatus.RUNNING);
    }
    
    /**
     * Test request has location filter. Location shows up in both requests. Return result should only contain the location we are interested.
     */
    @Test
    public void testWithMultipleLocationsFilter() {
         TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        RequestInfoHandler requestInfoHandler = mock(RequestInfoHandler.class);
        
        List<MitigationRequestDescriptionWithLocations> ongoingMitigationRequests = new ArrayList<>();
        ongoingMitigationRequests.add(REQUEST_1);
        ongoingMitigationRequests.add(REQUEST_2);
        List<String> filterLocations = Arrays.asList("C", "A");
        
        when(requestInfoHandler.getOngoingRequestsDescription(SERVICE_NAME, DEVICE_NAME, filterLocations, tsdMetrics)).thenReturn(ongoingMitigationRequests);
        
        MitigationInstanceInfoHandler instanceInfoHandler = mock(MitigationInstanceInfoHandler.class);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_1, tsdMetrics)).thenReturn(REQUEST_1_INSTANCES);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_2, tsdMetrics)).thenReturn(REQUEST_2_INSTANCES);
         
        OngoingRequestsFetcher ongoingRequestsFetcher = new OngoingRequestsFetcher(requestInfoHandler, SERVICE_NAME, DEVICE_NAME, filterLocations, instanceInfoHandler, tsdMetrics);
        
        List<MitigationRequestDescriptionWithStatuses> response = ongoingRequestsFetcher.call();
        // validate only specified locations status are returned
        // for missing location instance status, default RUNNING
        assertEquals(2, response.get(0).getInstancesStatusMap().size());
        assertEquals(1, response.get(1).getInstancesStatusMap().size());
        assertEquals(MitigationStatus.BLOCKED, response.get(0).getInstancesStatusMap().get("A").getMitigationStatus());
        assertEquals(MitigationStatus.BLOCKED, response.get(0).getInstancesStatusMap().get("C").getMitigationStatus());
        assertEquals(MitigationStatus.RUNNING, response.get(1).getInstancesStatusMap().get("C").getMitigationStatus());
    }
    
        
    /**
     * Test request has location filter. Location only shows up in one of the request. So should only return one request.
     */
    @Test
    public void testWithSingleLocationFilter() {
         TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        RequestInfoHandler requestInfoHandler = mock(RequestInfoHandler.class);
        
        List<MitigationRequestDescriptionWithLocations> ongoingMitigationRequests = new ArrayList<>();
        ongoingMitigationRequests.add(REQUEST_1);
        ongoingMitigationRequests.add(REQUEST_2);
        List<String> filterLocations = Arrays.asList("D");
        when(requestInfoHandler.getOngoingRequestsDescription(SERVICE_NAME, DEVICE_NAME, filterLocations, tsdMetrics)).thenReturn(ongoingMitigationRequests);
        
        MitigationInstanceInfoHandler instanceInfoHandler = mock(MitigationInstanceInfoHandler.class);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_1, tsdMetrics)).thenReturn(REQUEST_1_INSTANCES);
        when(instanceInfoHandler.getMitigationInstanceStatus(DEVICE_NAME, JOB_ID_2, tsdMetrics)).thenReturn(REQUEST_2_INSTANCES);
         
        OngoingRequestsFetcher ongoingRequestsFetcher = new OngoingRequestsFetcher(requestInfoHandler, SERVICE_NAME, DEVICE_NAME, filterLocations, instanceInfoHandler, tsdMetrics);
        
        List<MitigationRequestDescriptionWithStatuses> response = ongoingRequestsFetcher.call();
        // validate only specified location status is returned
        assertEquals(1, response.get(0).getInstancesStatusMap().size());
        assertEquals(MitigationStatus.BLOCKED, response.get(0).getInstancesStatusMap().get("D").getMitigationStatus());
    }
}
