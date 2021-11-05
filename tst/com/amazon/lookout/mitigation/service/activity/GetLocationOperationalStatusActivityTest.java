package com.amazon.lookout.mitigation.service.activity;

import amazon.mws.data.Datapoint;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.exception.ExternalDependencyException;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.GetLocationOperationalStatusRequest;
import com.amazon.lookout.mitigation.service.GetLocationOperationalStatusResponse;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedLocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.mws.MWSRequestException;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.spy;

public class GetLocationOperationalStatusActivityTest extends ActivityTestHelper {

    private static final String location = "brg-test-1";
    private GetLocationOperationalStatusRequest request;
    private GetLocationOperationalStatusActivity getLocationOperationalStatusActivity;
    private LocationStateInfoHandler locationStateInfoHandler;
    private LocationState locationState;

    @Before
    public void setup() {
        locationStateInfoHandler =
                spy(new DDBBasedLocationStateInfoHandler(locationStateDynamoDBHelper, activeMitigationsHelper, mwsHelper));

        getLocationOperationalStatusActivity =
                spy(new GetLocationOperationalStatusActivity(requestValidator, hostStatusInfoHandler, locationStateInfoHandler));

        request = new GetLocationOperationalStatusRequest();
        request.setLocation(location);
        locationState = LocationState.builder()
                .locationName(location)
                .adminIn(true)
                .inService(true)
                .build();
    }

    /**
     * Test GetLocationOperationalStatus Works for AdminIn
     */
    @Test
    public void testGetLocationOperationalStatusAdminIn() throws ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateInfoHandler)
                .checkIfLocationIsOperational(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new ArrayList<String>()).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertTrue(response.isIsOperational());
        assertEquals(new ArrayList<String>(), response.getPendingOperations());
    }

    /**
     * Test GetLocationOperationalStatus Works for AdminOut
     */
    @Test
    public void testGetLocationOperationalStatusAdminOut() throws ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();

        List<String> pendingOperations = new ArrayList<>();
        pendingOperations.add("MCM-1");
        GetLocationOperationalStatusResponse expectedResponse2 = new GetLocationOperationalStatusResponse();
        expectedResponse2.setIsOperational(false);
        expectedResponse2.setPendingOperations(pendingOperations);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(false).when(locationStateInfoHandler)
                .checkIfLocationIsOperational(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(pendingOperations).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertFalse(response.isIsOperational());
        assertEquals(pendingOperations, response.getPendingOperations());
    }

    /**
     * Test GetLocationOperationalStatus Works when all of the conditions for isOperational are statisfied
     */
    @Test
    public void testIfAllConditionsSatisfied() throws MWSRequestException, ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();


        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        //Expected Mitigations
        Mockito.doReturn(true).when(activeMitigationsHelper)
                .hasExpectedMitigations(eq(DeviceName.BLACKWATCH_BORDER), Mockito.anyListOf(BlackWatchLocation.class), eq(location));

        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        //RoutesAnnounced
        List<Datapoint> datapoints = new ArrayList<>();
        Datapoint datapoint1 = new Datapoint();
        datapoint1.setValue(20.0);
        datapoints.add(datapoint1);
        Datapoint datapoint2 = new Datapoint();
        datapoint2.setValue(20.0);
        datapoints.add(datapoint2);
        Datapoint datapoint3 = new Datapoint();
        datapoint3.setValue(20.0);
        datapoints.add(datapoint3);

        Mockito.doReturn(datapoints).when(mwsHelper)
                .getBGPTotalAnnouncements(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(true, response.isIsOperational());
        assertEquals(new ArrayList<String>(), response.getPendingOperations());
        assertEquals(new TreeMap<String, String>(), response.getOperationChanges());
    }


    /**
     * Test GetLocationOperationalStatus Works when ExpectedMitigations not present
     */
    @Test
    public void testIfExpectedMitigationsNotPresent() throws MWSRequestException, ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();
        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        List<BlackWatchLocation> neighbouringStacks = new ArrayList<>();
        BlackWatchLocation blackWatchLocation = new BlackWatchLocation();
        blackWatchLocation.setLocation("location-1");
        neighbouringStacks.add(blackWatchLocation);

        //Expected Mitigations
        Mockito.doReturn(false).when(activeMitigationsHelper)
                .hasExpectedMitigations(eq(DeviceName.BLACKWATCH_BORDER), eq(neighbouringStacks), eq(location));

        //RoutesAnnounced
        List<Datapoint> datapoints = new ArrayList<>();
        Datapoint datapoint1 = new Datapoint();
        datapoint1.setValue(20.0);
        datapoints.add(datapoint1);
        Datapoint datapoint2 = new Datapoint();
        datapoint2.setValue(20.0);
        datapoints.add(datapoint2);
        Datapoint datapoint3 = new Datapoint();
        datapoint3.setValue(20.0);
        datapoints.add(datapoint3);
        Mockito.doReturn(datapoints).when(mwsHelper)
                .getBGPTotalAnnouncements(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(false, response.isIsOperational());
        assertEquals(new TreeMap<String, String>(), response.getOperationChanges());
    }

    /**
     * Test GetLocationOperationalStatus Works when no routes announced not present
     */
    @Test
    public void testIfNoAnnouncedRoutes() throws MWSRequestException, ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();
        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        List<BlackWatchLocation> neighbouringStacks = new ArrayList<>();
        BlackWatchLocation blackWatchLocation = new BlackWatchLocation();
        blackWatchLocation.setLocation("location-1");
        neighbouringStacks.add(blackWatchLocation);

        //Expected Mitigations
        Mockito.doReturn(false).when(activeMitigationsHelper)
                .hasExpectedMitigations(eq(DeviceName.BLACKWATCH_BORDER), eq(neighbouringStacks), eq(location));

        //RoutesAnnounced
        List<Datapoint> datapoints = new ArrayList<>();
        Datapoint datapoint1 = new Datapoint();
        datapoint1.setValue(0);
        datapoints.add(datapoint1);
        Datapoint datapoint2 = new Datapoint();
        datapoint2.setValue(0);
        datapoints.add(datapoint2);
        Datapoint datapoint3 = new Datapoint();
        datapoint3.setValue(0);
        datapoints.add(datapoint3);
        Mockito.doReturn(datapoints).when(mwsHelper)
                .getBGPTotalAnnouncements(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(false, response.isIsOperational());
        assertEquals(new TreeMap<String, String>(), response.getOperationChanges());
    }

    /**
     * Test GetLocationOperationalStatus Works when inService=false
     */
    @Test
    public void testIfInServiceFalse() throws MWSRequestException, ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();
        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        LocationState locationState = LocationState.builder()
                .locationName(location)
                .adminIn(true)
                .inService(false)
                .build();

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        List<BlackWatchLocation> neighbouringStacks = new ArrayList<>();
        BlackWatchLocation blackWatchLocation = new BlackWatchLocation();
        blackWatchLocation.setLocation("location-1");
        neighbouringStacks.add(blackWatchLocation);

        //Expected Mitigations
        Mockito.doReturn(false).when(activeMitigationsHelper)
                .hasExpectedMitigations(eq(DeviceName.BLACKWATCH_BORDER), eq(neighbouringStacks), eq(location));

        //RoutesAnnounced
        List<Datapoint> datapoints = new ArrayList<>();
        Datapoint datapoint1 = new Datapoint();
        datapoint1.setValue(20.0);
        datapoints.add(datapoint1);
        Datapoint datapoint2 = new Datapoint();
        datapoint2.setValue(20.0);
        datapoints.add(datapoint2);
        Datapoint datapoint3 = new Datapoint();
        datapoint3.setValue(20.0);
        datapoints.add(datapoint3);
        Mockito.doReturn(datapoints).when(mwsHelper)
                .getBGPTotalAnnouncements(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(false, response.isIsOperational());
        assertEquals(new TreeMap<String, String>(), response.getOperationChanges());
    }

    /**
     * Test GetLocationOperationalStatus Works when location does not exist
     */
    @Test(expected = BadRequest400.class)
    public void testGetLocationOperationalStatusWhenLocationDoesNotExist() throws ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();

        Mockito.doReturn(null).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateInfoHandler)
                .checkIfLocationIsOperational(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new ArrayList<String>()).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new TreeMap<>()).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);
    }


    /**
     * Test GetLocationOperationalStatus Works when Operation Changes is present
     */
    @Test
    public void testGetLocationOperationalStatusWithOperationChanges() throws ExternalDependencyException {
        Mockito.doNothing().when(requestValidator).validateGetLocationOperationalStatusRequest(request);
        Mockito.doReturn(requestId).when(getLocationOperationalStatusActivity).getRequestId();

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateInfoHandler)
                .checkIfLocationIsOperational(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(new ArrayList<String>()).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        Map<String, String> mockOperationChanges = new TreeMap<>();
        mockOperationChanges.put("operationId1", "changeId1");

        Mockito.doReturn(mockOperationChanges).when(locationStateInfoHandler)
                .getOperationChanges(eq(location), isA(TSDMetrics.class));

        GetLocationOperationalStatusResponse response = getLocationOperationalStatusActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertTrue(response.isIsOperational());
        assertEquals(new ArrayList<String>(), response.getPendingOperations());
        assertEquals(1, response.getOperationChanges().size());
        assertEquals(mockOperationChanges, response.getOperationChanges());
    }
}