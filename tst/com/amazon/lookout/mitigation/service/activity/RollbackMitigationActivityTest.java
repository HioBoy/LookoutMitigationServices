package com.amazon.lookout.mitigation.service.activity;

import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import com.amazon.lookout.mitigation.exception.ExternalDependencyException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.mockito.Matchers.any;

import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;

import com.amazon.lookout.mitigation.RequestCreator;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

public class RollbackMitigationActivityTest extends ActivityTestHelper {
    private static RollbackMitigationActivity rollbackMitigationActivity;
    
    private RollbackMitigationRequest request;
    private RequestCreator requestCreator;
    
    @Before
    public void setupMore() {
        requestValidator = spy(new RequestValidator(
                "/random/path/location/json",
                mock(BwirCellConfig.class)));
        requestCreator = mock(RequestCreator.class);
        rollbackMitigationActivity = new RollbackMitigationActivity(requestValidator,
                mock(TemplateBasedRequestValidator.class),
                mock(RequestsDAO.class),
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
    public void testMissingMitigation() throws ExternalDependencyException {
        doThrow(new MissingMitigationVersionException404()).when(requestCreator)
            .queueRequest(any(), any(), any());
        rollbackMitigationActivity.enact(request);
    }
}

