package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeResponse;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class RequestHostStatusChangeActivityTest extends ActivityTestHelper {
    private RequestHostStatusChangeActivity requestHostStatusChangeActivity;
    private RequestHostStatusChangeRequest requestHostStatusChangeRequest;

    @Before
    public void setup() {
        requestHostStatusChangeActivity = spy(
                new RequestHostStatusChangeActivity(requestValidator, hostnameValidator, locationStateInfoHandler));
        requestHostStatusChangeRequest = new RequestHostStatusChangeRequest();
        requestHostStatusChangeRequest.setUserName(userArn);
        requestHostStatusChangeRequest.setReason("TEST");
        requestHostStatusChangeRequest.setRequestedStatus("ACTIVE");
        requestHostStatusChangeRequest.setLocation("brg-sfo99-1");
        requestHostStatusChangeRequest.setHostName("bw-brg-sfo99-1-58002.sfo9.amazon.com");
    }

    @Test
    public void testRequestHostStatusChangeActivity() {
        doReturn(requestId).when(requestHostStatusChangeActivity).getRequestId();
        doReturn(HostStatusEnum.ACTIVE)
                .when(requestValidator).validateRequestHostStatusChangeRequest(isA(RequestHostStatusChangeRequest.class));
        RequestHostStatusChangeResponse response =
                requestHostStatusChangeActivity.enact(requestHostStatusChangeRequest);
        assertEquals(response.getRequestId(), requestId);
        verify(requestHostStatusChangeActivity).getRequestId();
        verify(requestValidator).validateRequestHostStatusChangeRequest(any(RequestHostStatusChangeRequest.class));
    }
}
