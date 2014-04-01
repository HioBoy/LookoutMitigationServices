package com.amazon.lookout.mitigation.service.activity.validator.template;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.CompositeOrConstraint;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.RateLimitAction;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class Route53SingleCustomerMitigationValidationTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    private MitigationModificationRequest createMitigationModificationRequest() {
        MitigationModificationRequest request = new MitigationModificationRequest();
        request.setMitigationName("TestMitigationName");
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        request.setServiceName(ServiceName.Route53);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser("lookout");
        metadata.setToolName("lookoutui");
        metadata.setDescription("why not?");
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        request.setMitigationDefinition(definition);
        return request;
    }
    
    @Test
    public void testValidateRequestForTemplateHappyCase() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplate(request, request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    @Test
    public void testValidateRequestForUnknownDeviceScope() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        request.setMitigationTemplate("SomeRandomTemplate");
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplate(request, request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof InternalServerError500);
    }
    
    @Test
    public void testValidateRequestForTemplateAndDeviceFailure() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = mock(Route53SingleCustomerMitigationValidator.class);
        
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn(ServiceName.Route53);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        
        doThrow(new IllegalArgumentException()).when(route53SingleCustomerValidator).validateRequestForTemplateAndDevice(any(MitigationModificationRequest.class), anyString(), any(DeviceNameAndScope.class), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
            doCallRealMethod().when(route53SingleCustomerValidator).validateRequestForTemplate(any(MitigationModificationRequest.class), anyString(), any(TSDMetrics.class));
            route53SingleCustomerValidator.validateRequestForTemplate(request, request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenLocationsSpecified() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        request.setLocation(Lists.newArrayList("POP1", "POP2"));
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenActionTypeIsSpecified() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        request.getMitigationDefinition().setAction(new RateLimitAction());
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenCompositeConstraintIsSpecified() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        request.getMitigationDefinition().setConstraint(new CompositeOrConstraint());
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenConstraintAttributesIsNotRecognizable() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        SimpleConstraint simpleConstraint = new SimpleConstraint();
        simpleConstraint.setAttributeName("randomValue");
        request.getMitigationDefinition().setConstraint(simpleConstraint);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenConstraintIsNotOnDestIP() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        SimpleConstraint simpleConstraint = new SimpleConstraint();
        simpleConstraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
        request.getMitigationDefinition().setConstraint(simpleConstraint);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenConstraintHasMoreThanFourDestIPs() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        SimpleConstraint simpleConstraint = new SimpleConstraint();
        simpleConstraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        simpleConstraint.setAttributeValues(Lists.newArrayList("1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7", "5.6.7.8"));
        request.getMitigationDefinition().setConstraint(simpleConstraint);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenConstraintHasDestSubnets() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        SimpleConstraint simpleConstraint = new SimpleConstraint();
        simpleConstraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        simpleConstraint.setAttributeValues(Lists.newArrayList("1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7", "5.6.7.8/30"));
        request.getMitigationDefinition().setConstraint(simpleConstraint);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWhenConstraintHasDestIPsOtherThanRoute53() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationModificationRequest request = createMitigationModificationRequest();
        SimpleConstraint simpleConstraint = new SimpleConstraint();
        simpleConstraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        simpleConstraint.setAttributeValues(Lists.newArrayList("1.2.3.4", "2.3.4.5", "3.4.5.6", "4.5.6.7", "5.6.7.8"));
        request.getMitigationDefinition().setConstraint(simpleConstraint);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn(null);
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        
        reset(subnetsMatcher);
        when(subnetsMatcher.getServiceForSubnets(anyList())).thenReturn("CloudFront");
        
        caughtException = null;
        try {
            route53SingleCustomerValidator.validateRequestForTemplateAndDevice(request, request.getMitigationTemplate(), deviceNameAndScope, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    @Test
    public void testValidateCoexistenceForDifferentTemplates() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationDefinition definition1 = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        
        MitigationDefinition definition2 = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateCoexistenceForTemplateAndDevice("Template1", "Mitigation1", definition1, MitigationTemplate.Router_RateLimit_Route53Customer, 
                                                                                   "Mitigation2", definition2, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    @Test
    public void testValidateCoexistenceForSameTemplateDifferentDefinitions() {
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        Route53SingleCustomerMitigationValidator route53SingleCustomerValidator = new Route53SingleCustomerMitigationValidator(subnetsMatcher);
        
        MitigationDefinition definition1 = DDBBasedCreateRequestStorageHandlerTest.defaultCreateMitigationDefinition();
        MitigationDefinition definition2 = DDBBasedCreateRequestStorageHandlerTest.createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("9.8.7.6"));
        
        Throwable caughtException = null;
        try {
            route53SingleCustomerValidator.validateCoexistenceForTemplateAndDevice(MitigationTemplate.Router_RateLimit_Route53Customer, "Mitigation1", definition1, MitigationTemplate.Router_RateLimit_Route53Customer, 
                                                                                   "Mitigation2", definition2, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
    }
}
