package com.amazon.lookout.mitigation.service.activity;

import java.util.Arrays;

import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.lookout.mitigation.service.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ChangeBlackWatchMitigationStateActivityTest extends ActivityTestHelper {
    private ChangeBlackWatchMitigationStateRequest request;

    private ChangeBlackWatchMitigationStateActivity changeBlackWatchMitigationStateActivity;

    @Before
    public void setup() {
        changeBlackWatchMitigationStateActivity =
                Mockito.spy(new ChangeBlackWatchMitigationStateActivity(requestValidator, blackwatchMitigationInfoHandler));
        request = new ChangeBlackWatchMitigationStateRequest();
        request.setMitigationId("testMitigationId");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                        .withUser("Eren")
                        .withToolName("JUnit")
                        .withDescription("Test Descr")
                        .withRelatedTickets(Arrays.asList("1234", "5655"))
                        .build());
        request.setExpectedState(MitigationState.State.Active.name());
        request.setNewState(MitigationState.State.Expired.name());
    }

    @Test
    public void testChangeMitigationState() {
        Mockito.doReturn(requestId).when(changeBlackWatchMitigationStateActivity).getRequestId();

        ChangeBlackWatchMitigationStateResponse response = changeBlackWatchMitigationStateActivity.enact(request);
        Assert.assertEquals(request.getMitigationId(), response.getMitigationId());
        Assert.assertEquals(requestId, response.getRequestId());
    }
}

