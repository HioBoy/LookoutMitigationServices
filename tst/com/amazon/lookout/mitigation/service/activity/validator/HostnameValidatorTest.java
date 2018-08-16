package com.amazon.lookout.mitigation.service.activity.validator;

import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import org.junit.Before;
import org.junit.Test;

public class HostnameValidatorTest {

    HostnameValidator hostnameValidator;

    RequestHostStatusChangeRequest requestHostStatusChangeRequest;

    @Before
    public void setup() {
        hostnameValidator = new HostnameValidator();
        requestHostStatusChangeRequest = new RequestHostStatusChangeRequest();

        requestHostStatusChangeRequest.setRequestedStatus("ACTIVE");
        requestHostStatusChangeRequest.setReason("TEST");
        requestHostStatusChangeRequest.setUserName("DEVELOPER");
    }

    @Test
    public void validateHostnameTest1() {
        String hostname = "border-bw-2-01.lax1.blackwatch.iad.amazon.com";
        String location = "br-lax1-2";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test
    public void validateHostnameTest2() {
        String hostname = "edge-bw-c1-101.e-blr50.amazon.com";
        String location = "blr50-c1";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test
    public void validateHostnameTest3() {
        String hostname = "br-bw-1-04.ams1.blackwatch.eu-west-2.amazonaws.com";
        String location = "br-ams1-1";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void incompleteHostnameTest1() {
        String hostname = "border-bw-2-01.lax1.blackwatch.iad.amazon";
        String location = "br-ams1-1";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void incompleteHostnameTest2() {
        String hostname = "edge-bw-c1-101.amazon.com";
        String location = "blr50-c1";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void incompleteHostnameTest3() {
        String hostname = "ams1.blackwatch.eu-west-2.amazonaws.com";
        String location = "br-ams-1";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test(expected = IllegalArgumentException.class)
    public void incompleteHostnameTest4() {
        String hostname = "";
        String location = "";
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }

    @Test(expected = NullPointerException.class)
    public void incompleteHostnameTest5() {
        String hostname = null;
        String location = null;
        requestHostStatusChangeRequest.setHostName(hostname);
        requestHostStatusChangeRequest.setLocation(location);
        hostnameValidator.validateHostname(requestHostStatusChangeRequest);
    }
}

