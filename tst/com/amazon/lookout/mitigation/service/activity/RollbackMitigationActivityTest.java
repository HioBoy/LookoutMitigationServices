package com.amazon.lookout.mitigation.service.activity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Matchers.any;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.model.RequestType;

import com.amazon.lookout.mitigation.RequestCreator;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.WorkflowIdsDAO;
import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;

public class RollbackMitigationActivityTest extends ActivityTestHelper {
    private static RollbackMitigationActivity rollbackMitigationActivity;
    
    private RollbackMitigationRequest request;
    private RequestCreator requestCreator;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator(mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class),
                "/random/path/location/json"));
        requestCreator = mock(RequestCreator.class);
        rollbackMitigationActivity = new RollbackMitigationActivity(requestValidator,
                mock(TemplateBasedRequestValidator.class),
                mock(CurrentRequestsDAO.class),
                mock(ArchivedRequestsDAO.class),
                requestCreator);
        
        request = new RollbackMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setRollbackToMitigationVersion(rollbackMitigationVersion);
        request.setMitigationVersion(mitigationVersion);
        request.setMitigationTemplate(mitigationTemplate);
        request.setMitigationActionMetadata(mitigationActionMetadata);
        request.setDeviceName(DeviceName.BLACKWATCH_BORDER.name());
        request.setLocation(locations.get(0));
    }
    
    /**
     * Test invalid input null
     */
    @Test(expected = NullPointerException.class)
    public void testInvalidInput() {
        rollbackMitigationActivity.enact(null);
    }
    
    /**
     * Test invalid input, invalid mitigation name
     */
    @Test(expected = BadRequest400.class)
    public void testMissingParameters2() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        request.setMitigationName("invalid_mitigation_name\000");
        request.setRollbackToMitigationVersion(1000);
        
        rollbackMitigationActivity.enact(request);
    }
  
    /**
     * Test invalid input, missing rollback mitigation version
     */
    @Test(expected = BadRequest400.class)
    public void testMissingParameters3() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setRollbackToMitigationVersion(0);
        
        rollbackMitigationActivity.enact(request);
    }

    /**
     * Test missing mitigaiton case
     */
    @Test(expected = MissingMitigationVersionException404.class)
    public void testMissingMitigation() {
        doThrow(new MissingMitigationVersionException404()).when(requestCreator)
            .queueRequest(any(), any(), any());
        rollbackMitigationActivity.enact(request);
    }
}

