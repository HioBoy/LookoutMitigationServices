package com.amazon.lookout.mitigation.service.activity;

import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;

public class AbortDeploymentActivityTest extends ActivityTestHelper {
    private static AbortDeploymentActivity abortDeploymentActivity;
    
    private AbortDeploymentRequest request;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator("/random/path/location/json",
                mock(BwirCellConfig.class)));
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

