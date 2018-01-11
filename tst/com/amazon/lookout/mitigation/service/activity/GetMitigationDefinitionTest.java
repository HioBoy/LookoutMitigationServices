package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junitparams.JUnitParamsRunner;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.Parameters;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionResponse;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;

@RunWith(JUnitParamsRunner.class)
public class GetMitigationDefinitionTest extends ActivityTestHelper {
    private static GetMitigationDefinitionActivity getMitigationDefinitionActivity;
    private static final GetMitigationDefinitionRequest request = new GetMitigationDefinitionRequest();
    static {
        request.setDeviceName(deviceName);
        request.setMitigationName(mitigationName);
        request.setMitigationVersion(mitigationVersion);
    }
    
    @Before
    public void setupMore() {
        getMitigationDefinitionActivity = spy(new GetMitigationDefinitionActivity(
                requestValidator, mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class)));
    }
    
    /**
     * Test validation failure
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidInput() {
        doThrow(new IllegalArgumentException()).when(requestValidator).validateGetMitigationDefinitionRequest(request);
        getMitigationDefinitionActivity.enact(request);
    }
}

