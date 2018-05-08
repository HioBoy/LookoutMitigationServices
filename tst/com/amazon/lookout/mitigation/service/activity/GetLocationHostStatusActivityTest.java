package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import com.amazon.blackwatch.location.state.model.LocationState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusResponse;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;

public class GetLocationHostStatusActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private GetLocationHostStatusRequest request;
    
    private GetLocationHostStatusActivity getLocationHostStatusActivity;
    
    @Before
    public void setup() {
        getLocationHostStatusActivity =
                spy(new GetLocationHostStatusActivity(requestValidator, hostStatusInfoHandler,
                                                                              locationStateInfoHandler));

        request = new GetLocationHostStatusRequest();
        request.setLocation(location);
    }

    /**
     * Test host status retrieval works
     */
    @Test
    public void testLocationHostStatus() {
        Mockito.doNothing().when(requestValidator).validateGetLocationHostStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationHostStatusActivity).getRequestId();

        LocationState locationState = LocationState.builder()
                .locationName(location)
                .build();

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        List<HostStatusInLocation> listOfHostStatusInLocations = new ArrayList<>();
        HostStatusInLocation hostStatusinLocation = new HostStatusInLocation();
        hostStatusinLocation.setHostName("host1");
        hostStatusinLocation.setIsActive(true);
        listOfHostStatusInLocations.add(hostStatusinLocation);
        
        hostStatusinLocation = new HostStatusInLocation();
        hostStatusinLocation.setHostName("host2");
        hostStatusinLocation.setIsActive(false);
        listOfHostStatusInLocations.add(hostStatusinLocation);
        
        Mockito.doReturn(listOfHostStatusInLocations).when(hostStatusInfoHandler)
                .getHostsStatus(eq(locationState), isA(TSDMetrics.class));
        
        GetLocationHostStatusResponse response = getLocationHostStatusActivity.enact(request);

        assertEquals(listOfHostStatusInLocations, response.getListOfHostStatusesInLocation());
        assertEquals(location, response.getLocation());
        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Test empty list of host status retrieval works
     */
    @Test
    public void testEmptyLocationHostStatus() {
        LocationState locationState = LocationState.builder()
                .locationName(location)
                .build();
        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        List<HostStatusInLocation> listOfHostStatusInLocations = new ArrayList<>();
        Mockito.doReturn(listOfHostStatusInLocations).when(hostStatusInfoHandler)
                .getHostsStatus(eq(locationState), isA(TSDMetrics.class));

        GetLocationHostStatusResponse response = getLocationHostStatusActivity.enact(request);
        assertTrue(response.getListOfHostStatusesInLocation().isEmpty());
    }
    
    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        Mockito.doThrow(new IllegalArgumentException()).when(requestValidator).validateGetLocationHostStatusRequest(request);
        getLocationHostStatusActivity.enact(request);
    }
}