package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationOperation;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.location.type.LocationType;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.test.common.util.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;

public class DDBBasedLocationStateInfoHandlerTest {

    private static final String location1 = "brg-test99-1";
    private static final String location2 = "brg-test99-2";
    private static final String location3 = "brg-test99-3";
    private static final String location4 = "beg-test99-4";
    private LocationState locationState1;
    private LocationState locationState2;
    private LocationState locationState3;

    List<LocationState> allLocations;

    private final LocationStateDynamoDBHelper locationStateDynamoDBHelper = Mockito.mock(LocationStateDynamoDBHelper.class);

    private DDBBasedLocationStateInfoHandler ddbBasedLocationStateInfoHandler;

    private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    private static Metrics metrics = Mockito.mock(Metrics.class);
    private static TSDMetrics tsdMetrics;

    @Before
    public void setup() {
        TestUtils.configureLogging();

        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();
        tsdMetrics = new TSDMetrics(metricsFactory);

        LocationOperation firstLocationOperation = new LocationOperation();
        firstLocationOperation.setChangeId("change-id-1");

        LocationOperation secondLocationOperation = new LocationOperation();
        secondLocationOperation.setChangeId("change-id-2");

        Map<String, LocationOperation> operationsLocks = new TreeMap<>();
        operationsLocks.put("operation-id-1", firstLocationOperation);
        operationsLocks.put("operation-id-2", secondLocationOperation);

        locationState1 = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(true)
                .inService(true)
                .operationLocks(operationsLocks)
                .build();
        Mockito.doReturn(locationState1).when(locationStateDynamoDBHelper).getLocationState(eq(location1));

        locationState2 = LocationState.builder()
                .locationName(location2)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(true)
                .inService(true)
                .operationLocks(operationsLocks)
                .build();
        Mockito.doReturn(locationState2).when(locationStateDynamoDBHelper).getLocationState(eq(location2));

        locationState3 = LocationState.builder()
                .locationName(location3)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(true)
                .inService(true)
                .operationLocks(operationsLocks)
                .build();
        Mockito.doReturn(locationState3).when(locationStateDynamoDBHelper).getLocationState(eq(location3));

        allLocations = Arrays.asList(locationState1, locationState2, locationState3);
        Mockito.doReturn(allLocations).when(locationStateDynamoDBHelper).getAllLocationStates(anyInt());

        ddbBasedLocationStateInfoHandler = new DDBBasedLocationStateInfoHandler(locationStateDynamoDBHelper);
    }

    @Test
    public void testGetOperationChanges() {
        Map<String, String> operationChanges = ddbBasedLocationStateInfoHandler.getOperationChanges(location1, tsdMetrics);
        assertEquals(2, operationChanges.size());
        assertEquals("change-id-1", operationChanges.get("operation-id-1"));
        assertEquals("change-id-2", operationChanges.get("operation-id-2"));
    }

    @Test
    public void testGetOperationChanges_whenOperationLocksEmpty() {
        locationState1.setOperationLocks(new TreeMap<>());
        Mockito.doReturn(locationState1).when(locationStateDynamoDBHelper).getLocationState(eq(location1));
        Map<String, String> operationChanges = ddbBasedLocationStateInfoHandler.getOperationChanges(location1, tsdMetrics);
        assertNotNull(operationChanges);
        assertEquals(0, operationChanges.size());
    }

    @Test
    public void testLocationPairName() {
        assertEquals("brg-sea56-1", ddbBasedLocationStateInfoHandler.locationPairName("brg-sea56-2"));
        assertEquals("brg-sea56-2", ddbBasedLocationStateInfoHandler.locationPairName("brg-sea56-1"));
        assertEquals("br-sea56-1", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-2"));
        assertEquals("br-sea56-2", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-1"));
        assertEquals("br-sea56-3", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-4"));
        assertEquals("br-sea56-4", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-3"));
        assertEquals("br-sea56-11", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-12"));
        assertEquals("br-sea56-12", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-11"));
        assertEquals("br-sea56-f1-1", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-f1-2"));
        assertEquals("br-sea56-f1-2", ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-f1-1"));
        assertEquals("br-sea4-d2", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d1"));
        assertEquals("br-sea4-d1", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d2"));
        
        assertEquals("br-sea4-d9", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d10"));
        assertEquals("br-sea4-d10", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d9"));

        assertEquals("br-sea4-d11", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d12"));
        assertEquals("br-sea4-d12", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d11"));

        assertEquals("br-sea4-d99", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d100"));
        assertEquals("br-sea4-d100", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-d99"));

        assertEquals("br-sea4-9", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-10"));
        assertEquals("br-sea4-10", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-9"));

        assertEquals("br-sea4-11", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-12"));
        assertEquals("br-sea4-12", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-11"));

        assertEquals("br-sea4-99", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-100"));
        assertEquals("br-sea4-100", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-99"));

        assertEquals("br-sea4-f1-9", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-10"));
        assertEquals("br-sea4-f1-10", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-9"));

        assertEquals("br-sea4-f1-11", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-12"));
        assertEquals("br-sea4-f1-12", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-11"));

        assertEquals("br-sea4-f1-99", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-100"));
        assertEquals("br-sea4-f1-100", ddbBasedLocationStateInfoHandler.locationPairName("br-sea4-f1-99"));

        assertNull(ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-0"));
        assertNull(ddbBasedLocationStateInfoHandler.locationPairName("br-sea56-"));
        assertNull(ddbBasedLocationStateInfoHandler.locationPairName("br-sea-1"));
    }

    @Test
    public void testGetLocationsProtectingSameNetwork() {
        List<BlackWatchLocation> locations = ddbBasedLocationStateInfoHandler.getAllBlackWatchLocationsProtectingTheSameNetwork(locationState1, tsdMetrics);
        assertEquals(2, locations.size());

        Set<String> locationNames = locations.stream().map(BlackWatchLocation::getLocation).collect(Collectors.toSet());
        assertTrue(locationNames.contains(location1));
        assertTrue(locationNames.contains(location2));
        assertFalse(locationNames.contains(location3));

        /*
        * Test location3, which does not have a pair in the database (it's pair may have been removed or something)
        * In this case, we just return the original stack.
        * */
        locations = ddbBasedLocationStateInfoHandler.getAllBlackWatchLocationsProtectingTheSameNetwork(locationState3, tsdMetrics);
        assertEquals(1, locations.size());
        locationNames = locations.stream().map(BlackWatchLocation::getLocation).collect(Collectors.toSet());
        assertFalse(locationNames.contains(location1));
        assertFalse(locationNames.contains(location2));
        assertTrue(locationNames.contains(location3));
    }

    @Test
    public void testEvaluateOperationalFlags() {
        LocationState locationState = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(true)
                .inService(true)
                .build();
        assertTrue(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, true, true));
        assertFalse(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, false, true));
        assertFalse(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, true, false));

        locationState = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(true)
                .inService(false)
                .build();
        assertFalse(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, true, true));

        locationState = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(false)
                .inService(false)
                .build();
        assertTrue(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, true, true));
        assertFalse(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, false, true));
        locationState = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(false)
                .inService(true)
                .build();
        assertTrue(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, false, true));
        locationState = LocationState.builder()
                .locationName(location1)
                .locationType(LocationType.TC_BLACKWATCH15.name())
                .adminIn(false)
                .inService(false)
                .build();
        assertFalse(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, false, true));

	/*
         * Test for BE condition, ref SIM https://t.corp.amazon.com/D70547906/overview
         **/
        locationState = LocationState.builder()
                .locationName(location4)
                .locationType(LocationType.BPOP_BLACKWATCH15.name())
                .adminIn(true)
                .inService(true)
                .build();
        assertTrue(ddbBasedLocationStateInfoHandler.evaluateOperationalFlags(locationState, true, true));
    }

    //TODO: Add tests for other methods DDBBasedLocationStateInfoHandler
}
