package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;

public class ChangeBlackWatchMitigationOwnerARNActivityTest extends ActivityTestHelper {
    private ChangeBlackWatchMitigationOwnerARNRequest request;

    private ChangeBlackWatchMitigationOwnerARNActivity changeOwnerARNBlackWatchMitigationActivity;

    @Before
    public void setup() {
        changeOwnerARNBlackWatchMitigationActivity = 
                spy(new ChangeBlackWatchMitigationOwnerARNActivity(requestValidator, blackwatchMitigationInfoHandler));        
        request = new ChangeBlackWatchMitigationOwnerARNRequest();
        request.setMitigationId("testMitigationId");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());
    }

    @Test
    public void testChangeOwnerARNActivity() {
        Mockito.doReturn(requestId).when(changeOwnerARNBlackWatchMitigationActivity).getRequestId();
        
        ChangeBlackWatchMitigationOwnerARNResponse response = changeOwnerARNBlackWatchMitigationActivity.enact(request);
        assertEquals(request.getMitigationId(), response.getMitigationId());
        assertEquals(requestId, response.getRequestId());
    }
}
