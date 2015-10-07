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
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
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
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(serviceSubnetsMatcher,
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class));
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        request.setPreDeploymentChecks(null);
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateRequestForTemplate(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where we request validation for an invalid or a bad template.
     * We expect the validation to throw an exception here.
     */
    @Test
    public void testInvalidOrBadTemplateCase() {
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(serviceSubnetsMatcher,
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class));
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        request.setMitigationTemplate("BadTemplate");
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
     * Test the case where we have a request that doesn't pass the validation based on its template.
     * We expect the validation to throw back an exceptions here.
     */
    @Test
    public void testInvalidCase() {
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(serviceSubnetsMatcher.getServiceForSubnets(anyList())).thenReturn(null);
        
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(serviceSubnetsMatcher,
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class));
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateRequestForTemplate(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    /**
     * Test the case where we have a request that doesn't pass the validation based on its coexistence with an existing mitigation.
     * We expect the validation to throw back an exceptions here.
     */
    @Test
    public void testBadCoexistenceCase() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(subnetsMatcher,
                mock(EdgeLocationsHelper.class), mock(AmazonS3.class));
        
        MitigationDefinition definition1 = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        
        MitigationDefinition definition2 = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        
        Throwable caughtException = null;
        try {
            templateBasedValidator.validateCoexistenceForTemplateAndDevice(MitigationTemplate.Router_RateLimit_Route53Customer, "Mitigation1", definition1, 
                                                                           MitigationTemplate.Router_RateLimit_Route53Customer, "Mitigation2", definition2);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
    }
}
