package com.amazon.lookout.mitigation.service.activity.validator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Lists;

public class RequestValidatorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    private final String mitigationName = "TestMitigationName";
    private final String mitigationTemplate = MitigationTemplate.Router_RateLimit_Route53Customer;
    private final String serviceName = ServiceName.Route53;
    private final String userName = "TestUserName";
    private final String toolName = "TestToolName";
    private final String description = "TestDesc";
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    /**
     * Test the case where the request is completely valid. We expect the validation to be completed without any exceptions.
     */
    @Test
    public void testHappyCase() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Null or empty mitigation name"));
    }
    
    /**
     * Test the case where the request is missing the mitigationTemplate
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingOrInvalidMitigationTemplate() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().startsWith("Null or empty mitigation template"));
        
        request.setMitigationTemplate("BadTemplate");
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid mitigation template found"));
    }
    
    /**
     * Test the case where the request is missing the serviceName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingOrInvalidServiceName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Null or empty service name"));
        
        request.setServiceName("BadServiceName");
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid service name found"));
    }
    
    /**
     * Test the case where the request is missing the mitigationDefinition
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationActionMetadata() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("No MitigationActionMetadata found"));
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingUserName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setToolName(toolName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("No user defined"));
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingToolName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setDescription(description);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("No tool specified in the mitigation action metadata"));
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationDescription() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        request.setMitigationActionMetadata(metadata);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("No description specified in the mitigation action metadata"));
    }
    
    /**
     * Test the happy case for a delete request. We don't expect any exception to be thrown in this case.
     */
    @Test
    public void testValidateDeleteRequestHappyCase() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        request.setMitigationVersion(2);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        request.setMitigationActionMetadata(metadata);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateDeleteRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case for a delete request with no mitigation version provided as input. 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testDeleteRequestWithInvalidMitigationVersion() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        request.setMitigationActionMetadata(metadata);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateDeleteRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Version of the mitigation to be deleted should be set to >=1"));
    }
    
    /**
     * Test the case for a create request with duplicates in the related tickets. 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testCreateRequestWithDuplicateRelatedTickets() {
        CreateMitigationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        metadata.setRelatedTickets(Lists.newArrayList("Tkt1", "Tkt2", "Tkt2"));
        request.setMitigationActionMetadata(metadata);
        
        RequestValidator validator = new RequestValidator();
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Duplicate related tickets found in actionMetadata"));
    }
}