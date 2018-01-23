package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.blackwatch.location.state.model.LocationType;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateResponse;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateRequest;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;

public class UpdateBlackWatchLocationStateActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private static final String reason = "winter";
    private UpdateBlackWatchLocationStateRequest request;
    
    private UpdateBlackWatchLocationStateActivity updateBlackWatchLocationStateActivity;
    
    @Before
    public void setup() {
        requestValidator = spy(new RequestValidator(
                "/random/path/location/json"));
        updateBlackWatchLocationStateActivity = 
                spy(new UpdateBlackWatchLocationStateActivity(requestValidator, locationStateInfoHandler));
        
        request = new UpdateBlackWatchLocationStateRequest();
        request.setLocation(location);
        request.setReason(reason);
        request.setAdminIn(true);
        request.setLocationType(LocationType.POP_HAPUNA12.name());
    }

    /**
     * Test simple valid flow 
     */
    @Test
    public void testUpdateAdminInState() {
        Mockito.doReturn(requestId).when(updateBlackWatchLocationStateActivity).getRequestId();
        UpdateBlackWatchLocationStateResponse response = updateBlackWatchLocationStateActivity.enact(request);

        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        Mockito.doThrow(new IllegalArgumentException()).when(requestValidator).validateUpdateBlackWatchLocationStateRequest(request);
        updateBlackWatchLocationStateActivity.enact(request);
    }
    
    /**
     * Test invalid request, invalid location name. location.length() > 100
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidLocationName() {
        request.setLocation("LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum");
        updateBlackWatchLocationStateActivity.enact(request);
    }
    
    /**
     * Test invalid request, invalid change reason. changeReason.length() > 500
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidChangeReason() {
        request.setReason("LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum"
                + "LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum"
                + "LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum"
                + "LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum"
                + "LoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsumLoralIpsum");
        updateBlackWatchLocationStateActivity.enact(request);
    }
    
    /**
     * Test invalid request, invalid location type.
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidLocationType() {
        request.setLocationType("randomLocationType");
        updateBlackWatchLocationStateActivity.enact(request);
    }
}
