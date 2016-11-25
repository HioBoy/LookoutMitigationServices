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
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;

public class ApplyBlackWatchMitigationActivityTest extends ActivityTestHelper {
    private ApplyBlackWatchMitigationRequest request;

    private ApplyBlackWatchMitigationActivity applyBlackWatchMitigationActivity;

    @Before
    public void setup() {
        TestUtils.configureLogging();
        applyBlackWatchMitigationActivity = 
                spy(new ApplyBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));
        identity.setAttribute(Identity.AWS_USER_ARN, "arn:12324554");
        request = new ApplyBlackWatchMitigationRequest();
        request.setResourceId("AddressList1234");
        request.setResourceType("IPAddressList");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());
    }

    @Test
    public void testApplyBlackWatchMitigationActivity() {
        //applyBWMitigation Doesn't do much, so we only test the functionality that the activity directly adds.
        //The other areas are tested with the handler.
        Mockito.doReturn(requestId).when(applyBlackWatchMitigationActivity).getRequestId();
        Mockito.doReturn(identity).when(applyBlackWatchMitigationActivity).getIdentity();
        
        ApplyBlackWatchMitigationResponse retResponse = new ApplyBlackWatchMitigationResponse();
        Mockito.doReturn(retResponse).when(blackwatchMitigationInfoHandler).applyBlackWatchMitigation(anyString(), 
                anyString(), anyLong(), anyLong(), anyInt(), 
                isA(MitigationActionMetadata.class), anyString(), anyString(), isA(TSDMetrics.class));

        ApplyBlackWatchMitigationResponse response = applyBlackWatchMitigationActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
    }
}
