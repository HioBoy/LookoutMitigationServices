package com.amazon.lookout.mitigation.service.activity.validator.template;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazonaws.services.s3.AmazonS3;

@SuppressWarnings("unchecked")
public class TemplateBasedRequestValidatorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    /**
     * Test the case where we have a valid request using a valid template.
     * We expect the validation to pass without any exceptions here.
     */
    @Test
    public void testHappyCase() {
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class));
        MitigationModificationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        request.setPreDeploymentChecks(null);
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateRequestForTemplate(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
            ex.printStackTrace();
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where we request validation for an invalid or a bad template.
     * We expect the validation to throw an exception here.
     */
    @Test
    public void testInvalidOrBadTemplateCase() {
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class));
        MitigationModificationRequest request = 
                RequestTestHelper.generateCreateMitigationRequest("BadTemplate", "Name");
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateRequestForTemplate(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof InternalServerError500);
        assertTrue(caughtException.getMessage().startsWith("No check configured for mitigationTemplate"));
    }
    
    /**
     * Test the case where we have a request that doesn't pass the validation based on its coexistence with an existing mitigation.
     * We expect the validation to throw back an exceptions here.
     */
    @Test
    public void testBadCoexistenceCase() {
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class));
        
        MitigationDefinition definition1 = RequestTestHelper.defaultMitigationDefinition();
        
        MitigationDefinition definition2 = RequestTestHelper.defaultMitigationDefinition();
        
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateCoexistenceForTemplateAndDevice(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, "Mitigation1", definition1, 
                                                                           MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, "Mitigation2", definition2,
                                                                           tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);  // BlackWatch templates allow mitigations to coexist
    }
}
