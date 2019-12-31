package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.UpdateLocationStateRequest;
import com.amazon.lookout.mitigation.service.UpdateLocationStateResponse;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.spy;

public class UpdateLocationStateActivityTest extends ActivityTestHelper {

    private static final String location = "brg-test-1";
    private UpdateLocationStateRequest request;
    private UpdateLocationStateActivity updateLocationStateActivity;
    private LocationState locationState;

    @Before
    public void setup() {

        updateLocationStateActivity =
                spy(new UpdateLocationStateActivity(requestValidator, locationStateInfoHandler, hostnameValidator, locationStateLocksDAO));

        request = new UpdateLocationStateRequest();
        request.setLocation(location);

        locationState = LocationState.builder()
                .locationName(location)
                .adminIn(true)
                .inService(true)
                .build();
    }

    /**
     * Test UpdateLocationStateActivity for AdminIn
     * AcquireLock successful
     */
    @Test
    public void testUpdateLocationStateAdminInAcquireLock() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        //AdminIn Request
        request.setChangeId("adminIn-1");
        request.setOperationId("adminInOperation");
        request.setAdminIn(true);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        Mockito.doReturn(true).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        Mockito.doReturn(new ArrayList<String>()).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        UpdateLocationStateResponse response = updateLocationStateActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(new ArrayList<String>(), response.getPendingOperations());
    }

    /**
     * Test UpdateLocationStateActivity for AdminIn
     * AcquireLock unsuccessful
     */
    @Test(expected = InternalServerError500.class)
    public void testUpdateLocationStateAdminInAcquireLockUnsuccessful() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        //AdminIn Request
        request.setChangeId("adminIn-1");
        request.setOperationId("adminInOperation");
        request.setAdminIn(true);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doThrow(new RuntimeException()).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        updateLocationStateActivity.enact(request);
    }

    /**
     * Test UpdateLocationStateActivity for AdminOut
     * AcquireLock successful
     * OtherStacks Inservice
     */
    @Test
    public void testUpdateLocationStateAdminOutAcquireLock() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        String changeId = "adminOut";
        String operationId = "adminOutOperation";
        //AdminOut Request
        request.setChangeId(changeId);
        request.setOperationId(operationId);
        request.setAdminIn(false);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateInfoHandler)
                .validateOtherStacksInService(eq(location), isA(TSDMetrics.class));

        List<String> pendingLocks = Arrays.asList(operationId);

        Mockito.doReturn(pendingLocks).when(locationStateInfoHandler)
                .getPendingOperationLocks(eq(location), isA(TSDMetrics.class));

        UpdateLocationStateResponse response = updateLocationStateActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
        assertEquals(pendingLocks, response.getPendingOperations());
    }

    /**
     * Test UpdateLocationStateActivity for AdminOut
     * AcquireLock successful
     * OtherStacks Inservice
     */
    @Test(expected = BadRequest400.class)
    public void testUpdateLocationStateAdminOutNoStackInService() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        String changeId = "adminOut";
        String operationId = "adminOutOperation";
        //AdminOut Request
        request.setChangeId(changeId);
        request.setOperationId(operationId);
        request.setAdminIn(false);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        Mockito.doReturn(false).when(locationStateInfoHandler)
                .validateOtherStacksInService(eq(location), isA(TSDMetrics.class));

        updateLocationStateActivity.enact(request);
    }

    /**
     * Test UpdateLocationStateActivity for AdminOut
     * AcquireLock unsuccessful
     */
    @Test(expected = InternalServerError500.class)
    public void testUpdateLocationStateAdminOutAcquireLockUnsuccessful() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        String changeId = "adminOut";
        String operationId = "adminOutOperation";
        //AdminOut Request
        request.setChangeId(changeId);
        request.setOperationId(operationId);
        request.setAdminIn(false);

        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doThrow(new RuntimeException()).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        updateLocationStateActivity.enact(request);
    }

    /**
     * Test UpdateLocationStateActivity when other stack processing
     */
    @Test(expected = BadRequest400.class)
    public void testUpdateLocationStateAdminOutWhenOtherStackProcessing() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        String changeId = "adminOut";
        String operationId = "adminOutOperation";
        //AdminOut Request
        request.setChangeId(changeId);
        request.setOperationId(operationId);
        request.setAdminIn(false);
        request.setLocation(location);

        String locationNeighboringLocation = "brg-test-2";
        LocationState location2State = LocationState.builder()
                .locationName(location)
                .adminIn(false)
                .inService(true)
                .build();


        Mockito.doReturn(locationState).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        Mockito.doReturn(true).when(locationStateLocksDAO)
                .acquireWriterLock(eq(DeviceName.BLACKWATCH_BORDER), eq(location));

        Mockito.doReturn(location2State).when(locationStateInfoHandler)
                .getLocationState(eq(locationNeighboringLocation), isA(TSDMetrics.class));

        updateLocationStateActivity.enact(request);
    }

    /**
     * Test UpdateLocationStateActivity for when stack does not exist
     */
    @Test(expected = BadRequest400.class)
    public void testUpdateLocationStateAdminOutWhenStackDoesNotExist() {
        Mockito.doNothing().when(requestValidator).validateUpdateLocationStateRequest(request);
        Mockito.doReturn(requestId).when(updateLocationStateActivity).getRequestId();

        String changeId = "adminOut";
        String operationId = "adminOutOperation";
        //AdminOut Request
        request.setChangeId(changeId);
        request.setOperationId(operationId);
        request.setAdminIn(false);
        request.setLocation(location);

        Mockito.doReturn(null).when(locationStateInfoHandler)
                .getLocationState(eq(location), isA(TSDMetrics.class));

        updateLocationStateActivity.enact(request);
    }
}
