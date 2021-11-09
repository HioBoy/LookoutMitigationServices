package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationOperation;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.ActiveMitigationsHelper;
import com.amazon.lookout.mitigation.service.GetLocationOperationalStatusRequest;
import com.amazon.lookout.mitigation.service.activity.helper.mws.MWSHelper;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.utils.DynamoDBLocalMocks;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;

public class DDBBasedLocationStateInfoHandlerTest {

    private static final String location = "brg-test-1";
    private LocationState locationState;

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

        locationState = LocationState.builder()
                .locationName(location)
                .adminIn(true)
                .inService(true)
                .operationLocks(operationsLocks)
                .build();

        Mockito.doReturn(locationState).when(locationStateDynamoDBHelper).getLocationState(eq(location));

        ddbBasedLocationStateInfoHandler = new DDBBasedLocationStateInfoHandler(locationStateDynamoDBHelper);
    }

    @Test
    public void testGetOperationChanges() {
        Map<String, String> operationChanges = ddbBasedLocationStateInfoHandler.getOperationChanges(location, tsdMetrics);
        assertEquals(2, operationChanges.size());
        assertEquals("change-id-1", operationChanges.get("operation-id-1"));
        assertEquals("change-id-2", operationChanges.get("operation-id-2"));
    }

    @Test
    public void testGetOperationChanges_whenOperationLocksEmpty() {
        locationState.setOperationLocks(new TreeMap<>());
        Mockito.doReturn(locationState).when(locationStateDynamoDBHelper).getLocationState(eq(location));
        Map<String, String> operationChanges = ddbBasedLocationStateInfoHandler.getOperationChanges(location, tsdMetrics);
        assertNotNull(operationChanges);
        assertEquals(0, operationChanges.size());
    }

    //TODO: Add tests for other methods DDBBasedLocationStateInfoHandler
}
