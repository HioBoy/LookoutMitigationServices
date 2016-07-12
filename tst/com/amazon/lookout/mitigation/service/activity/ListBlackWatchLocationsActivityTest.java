package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsResponse;
import com.amazon.lookout.mitigation.service.BlackWatchLocations;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;

public class ListBlackWatchLocationsActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private ListBlackWatchLocationsRequest request;
    
    private ListBlackWatchLocationsActivity listBlackWatchLocationsActivity;
    
    @Before
    public void setup() {
        listBlackWatchLocationsActivity = 
                spy(new ListBlackWatchLocationsActivity(locationStateInfoHandler));
        
        request = new ListBlackWatchLocationsRequest();
    }

    /**
     * Test host status retrieval works
     */
    @Test
    public void testLocationHostStatus() {
        Mockito.doReturn(requestId).when(listBlackWatchLocationsActivity).getRequestId();
        
        List<BlackWatchLocations> listOfBlackWatchLocations = new ArrayList<>();
        BlackWatchLocations blackWatchLocations = new BlackWatchLocations();
        blackWatchLocations.setLocation("location1");
        blackWatchLocations.setAdminIn(true);
        listOfBlackWatchLocations.add(blackWatchLocations);
        
        blackWatchLocations = new BlackWatchLocations();
        blackWatchLocations.setLocation("location2");
        blackWatchLocations.setAdminIn(false);
        listOfBlackWatchLocations.add(blackWatchLocations);
        
        Mockito.doReturn(listOfBlackWatchLocations).when(locationStateInfoHandler).getBlackWatchLocations(isA(TSDMetrics.class));
        
        ListBlackWatchLocationsResponse response = listBlackWatchLocationsActivity.enact(request);

        assertEquals(listOfBlackWatchLocations, response.getListOfLocationsAndAdminState());
        assertEquals(requestId, response.getRequestId());
    }
    /**
     * Test empty list of host status retrieval works
     */
    @Test
    public void testEmptyLocationHostStatus() {
       List<BlackWatchLocations> listOfBlackWatchLocations = new ArrayList<>();
        Mockito.doReturn(listOfBlackWatchLocations).when(locationStateInfoHandler).getBlackWatchLocations(isA(TSDMetrics.class));
        ListBlackWatchLocationsResponse response = listBlackWatchLocationsActivity.enact(request);
        assertTrue(response.getListOfLocationsAndAdminState ().isEmpty());
    }
}