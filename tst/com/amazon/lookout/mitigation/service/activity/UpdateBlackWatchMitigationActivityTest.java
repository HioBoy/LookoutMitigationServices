package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;

public class UpdateBlackWatchMitigationActivityTest extends ActivityTestHelper {
    private UpdateBlackWatchMitigationRequest request;

    private UpdateBlackWatchMitigationActivity updateBlackWatchMitigationActivity;

    @Before
    public void setup() {
        TestUtils.configureLogging();
        updateBlackWatchMitigationActivity = 
                spy(new UpdateBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));
        identity.setAttribute(Identity.AWS_USER_ARN, "arn:12324554");
        request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationId("");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234,5655"))
                .build());
    }

    @Test
    public void testUpdateBlackWatchMitigationActivity() {
        //updateBWMitigation Doesn't do much, so we only test the functionality that the activity directly adds.
        //The other areas are tested with the handler.
        Mockito.doReturn(requestId).when(updateBlackWatchMitigationActivity).getRequestId();
        Mockito.doReturn(identity).when(updateBlackWatchMitigationActivity).getIdentity();
        
        UpdateBlackWatchMitigationResponse retResponse = new UpdateBlackWatchMitigationResponse();
        Mockito.doReturn(retResponse).when(blackwatchMitigationInfoHandler).updateBlackWatchMitigation(anyString(), 
                anyLong(), anyLong(), anyInt(), 
                isA(MitigationActionMetadata.class), anyString(), anyString(), isA(TSDMetrics.class));
        
        UpdateBlackWatchMitigationResponse response = updateBlackWatchMitigationActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
    }
}
