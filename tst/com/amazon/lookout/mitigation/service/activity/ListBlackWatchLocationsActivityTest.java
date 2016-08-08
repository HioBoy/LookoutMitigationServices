package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsResponse;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;

public class ListBlackWatchLocationsActivityTest extends ActivityTestHelper {
    private ListBlackWatchLocationsRequest request;
    
    private ListBlackWatchLocationsActivity listBlackWatchLocationActivity;
    
    @Before
    public void setup() {
        listBlackWatchLocationActivity = 
                spy(new ListBlackWatchLocationsActivity(requestValidator, locationStateInfoHandler));
        
        request = new ListBlackWatchLocationsRequest();
        request.setRegion("region");
    }

    /**
     * Test host status retrieval works
     */
    @Test
    public void testLocationHostStatus() {
        Mockito.doReturn(requestId).when(listBlackWatchLocationActivity).getRequestId();
        
        List<BlackWatchLocation> listOfBlackWatchLocation = new ArrayList<>();
        BlackWatchLocation blackWatchLocations = new BlackWatchLocation();
        blackWatchLocations.setLocation("location1");
        blackWatchLocations.setAdminIn(true);
        listOfBlackWatchLocation.add(blackWatchLocations);
        
        blackWatchLocations = new BlackWatchLocation();
        blackWatchLocations.setLocation("location2");
        blackWatchLocations.setAdminIn(false);
        listOfBlackWatchLocation.add(blackWatchLocations);
        
        Mockito.doReturn(listOfBlackWatchLocation).when(locationStateInfoHandler).getBlackWatchLocation(eq("region"), isA(TSDMetrics.class));
        
        ListBlackWatchLocationsResponse response = listBlackWatchLocationActivity.enact(request);

        assertEquals(listOfBlackWatchLocation, response.getListOfLocationsAndAdminState());
        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Test empty list of host status retrieval works
     */
    @Test
    public void testEmptyLocationHostStatus() {
       List<BlackWatchLocation> listOfBlackWatchLocation = new ArrayList<>();
        Mockito.doReturn(listOfBlackWatchLocation).when(locationStateInfoHandler).getBlackWatchLocation(eq("region"), isA(TSDMetrics.class));
        ListBlackWatchLocationsResponse response = listBlackWatchLocationActivity.enact(request);
        assertTrue(response.getListOfLocationsAndAdminState ().isEmpty());
    }
}