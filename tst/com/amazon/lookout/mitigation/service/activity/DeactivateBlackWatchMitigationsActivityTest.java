package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;

public class DeactivateBlackWatchMitigationsActivityTest extends ActivityTestHelper {
    private DeactivateBlackWatchMitigationRequest request;

    private DeactivateBlackWatchMitigationActivity deactivateBlackWatchMitigationActivity;

    @Before
    public void setup() {
        deactivateBlackWatchMitigationActivity = 
                spy(new DeactivateBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));        
        request = new DeactivateBlackWatchMitigationRequest();
        request.setMitigationId("testMitigationId");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());
    }

    /**
     * Test list blackwatch mitigation activity works
     */
    @Test
    public void testListBlackWatchMitigationsActivity() {
        Mockito.doReturn(requestId).when(deactivateBlackWatchMitigationActivity).getRequestId();
        
        DeactivateBlackWatchMitigationResponse response = deactivateBlackWatchMitigationActivity.enact(request);
        assertEquals(request.getMitigationId(), response.getMitigationId());
        assertEquals(requestId, response.getRequestId());
    }
}
