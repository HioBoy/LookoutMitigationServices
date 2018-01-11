package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryResponse;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;

public class GetLocationDeploymentHistoryActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private GetLocationDeploymentHistoryRequest request;
    
    private GetLocationDeploymentHistoryActivity getLocationDeploymentHistoryActivity;
    
    @Before
    public void setup() {
        getLocationDeploymentHistoryActivity = 
                spy(new GetLocationDeploymentHistoryActivity(requestValidator,
                            mock(CurrentRequestsDAO.class),
                            mock(ArchivedRequestsDAO.class)));
        
        request = new GetLocationDeploymentHistoryRequest();
        request.setDeviceName(deviceName);
        request.setLocation(location);
        request.setMaxNumberOfHistoryEntriesToFetch(maxNumberOfHistoryEntriesToFetch);
    }

    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        Mockito.doThrow(new IllegalArgumentException()).when(requestValidator).validateGetLocationDeploymentHistoryRequest(request);
        getLocationDeploymentHistoryActivity.enact(request);
    }
}

