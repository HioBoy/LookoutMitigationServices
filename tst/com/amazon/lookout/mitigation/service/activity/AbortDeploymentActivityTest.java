package com.amazon.lookout.mitigation.service.activity;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.AbortDeploymentResponse;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;

public class AbortDeploymentActivityTest extends ActivityTestHelper {
    private static AbortDeploymentActivity abortDeploymentActivity;
    
    private AbortDeploymentRequest request;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator(
                mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class),
                "/random/path/location/json"));
        abortDeploymentActivity = new AbortDeploymentActivity(requestValidator,
                mock(CurrentRequestsDAO.class));
    }
    
    /**
     * Test invalid null request
     */
    @Test(expected = NullPointerException.class)
    public void testNullRequest() {
    	abortDeploymentActivity.enact(null);
    }
    
    /**
     * Test invalid request, invalid jobid
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidJobID() {
        request = new AbortDeploymentRequest();
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName(deviceName);
        request.setJobId(0);        
        abortDeploymentActivity.enact(request);
    }

    /**
     * Test invalid request, invalid device name
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidDeviceName() {
        request = new AbortDeploymentRequest();
        request.setMitigationTemplate(mitigationTemplate);
        request.setDeviceName("InvalidDeviceName");
        request.setJobId(workflowId);    
        abortDeploymentActivity.enact(request);
    }

    /**
     * Test invalid request, invalid mitigation template
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidMitigationTemplate() {
        request = new AbortDeploymentRequest();
        request.setMitigationTemplate("InvalidTemplate");
        request.setDeviceName(deviceName);
        request.setJobId(workflowId);    
        abortDeploymentActivity.enact(request);
    }
}

