package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeResponse;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.*;

public class RequestHostStatusChangeActivityTest extends ActivityTestHelper {
    private static final String changeUser = "test-user";
    private static final String changeReason = "Test";
    private static final String activeStatus = "ACTIVE";
    private static final String disabledStatus = "DISABLED";
    private static final String location = "brg-sfo99-1";
    private static final String validHostname = "bw-brg-sfo99-1-58002.sfo9.amazon.com";
    private static final String invalidHostname = "bw-route-gamma-4102.pdx4-1.ec2.substrate";
    private static final List<String> relatedLinks = ImmutableList.of("http://www.example.com/test");

    private RequestHostStatusChangeActivity requestHostStatusChangeActivity;

    @Before
    public void setup() {
        requestHostStatusChangeActivity = spy(
                new RequestHostStatusChangeActivity(requestValidator, hostnameValidator, locationStateInfoHandler));
        doReturn(requestId).when(requestHostStatusChangeActivity).getRequestId();
    }

    @Test
    public void testRequestHostStatusChangeActivity_ValidRequest() {
        RequestHostStatusChangeRequest request = new RequestHostStatusChangeRequest();
        request.setUserName(changeUser);
        request.setReason(changeReason);
        request.setRequestedStatus(activeStatus);
        request.setLocation(location);
        request.setHostName(validHostname);
        request.setRelatedLinks(relatedLinks);
        doReturn(HostStatusEnum.ACTIVE).when(requestValidator).validateRequestHostStatusChangeRequest(request);
        RequestHostStatusChangeResponse response =
                requestHostStatusChangeActivity.enact(request);
        assertEquals(response.getRequestId(), requestId);
        verify(requestHostStatusChangeActivity).getRequestId();
        verify(requestValidator).validateRequestHostStatusChangeRequest(request);
        verify(hostnameValidator).validateHostname(request);
        verify(locationStateInfoHandler).requestHostStatusChange(eq(location), eq(validHostname),
                eq(HostStatusEnum.ACTIVE), eq(changeReason), eq(changeUser), anyString(), eq(relatedLinks),
                eq(true), any(TSDMetrics.class));
    }

    @Test
    public void testRequestHostStatusChangeActivity_InvalidHostname_HostFound() {
        RequestHostStatusChangeRequest request = new RequestHostStatusChangeRequest();
        request.setUserName(changeUser);
        request.setReason(changeReason);
        request.setRequestedStatus(disabledStatus);
        request.setLocation(location);
        request.setHostName(invalidHostname);
        request.setRelatedLinks(relatedLinks);
        doReturn(HostStatusEnum.DISABLED).when(requestValidator).validateRequestHostStatusChangeRequest(request);
        doThrow(new IllegalArgumentException("invalid hostname")).when(hostnameValidator).validateHostname(request);
        RequestHostStatusChangeResponse response =
                requestHostStatusChangeActivity.enact(request);
        assertEquals(response.getRequestId(), requestId);
        verify(requestValidator).validateRequestHostStatusChangeRequest(request);
        verify(hostnameValidator).validateHostname(request);
        verify(locationStateInfoHandler).requestHostStatusChange(eq(location), eq(invalidHostname),
                eq(HostStatusEnum.DISABLED), eq(changeReason), eq(changeUser), anyString(), eq(relatedLinks),
                eq(false), any(TSDMetrics.class));
    }

    @Test(expected = BadRequest400.class)
    public void testRequestHostStatusChangeActivity_InvalidHostname_HostNotFound() {
        RequestHostStatusChangeRequest request = new RequestHostStatusChangeRequest();
        request.setUserName(changeUser);
        request.setReason(changeReason);
        request.setRequestedStatus(disabledStatus);
        request.setLocation(location);
        request.setHostName(invalidHostname);
        request.setRelatedLinks(relatedLinks);
        doReturn(HostStatusEnum.DISABLED).when(requestValidator).validateRequestHostStatusChangeRequest(request);
        doThrow(new IllegalArgumentException("invalid hostname")).when(hostnameValidator).validateHostname(request);
        doThrow(new IllegalArgumentException("host not found")).when(locationStateInfoHandler)
                .requestHostStatusChange(eq(location), eq(invalidHostname),
                        eq(HostStatusEnum.DISABLED), eq(changeReason), eq(changeUser), anyString(), eq(relatedLinks),
                        eq(false), any(TSDMetrics.class));
        requestHostStatusChangeActivity.enact(request); // should throw
    }
}
