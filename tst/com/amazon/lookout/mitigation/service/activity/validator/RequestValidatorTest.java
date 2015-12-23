package com.amazon.lookout.mitigation.service.activity.validator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RequestValidatorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    private final String mitigationName = "TestMitigationName";
    private final String rateLimitMitigationTemplate = MitigationTemplate.Router_RateLimit_Route53Customer;
    private final String countModeMitigationTemplate = MitigationTemplate.Router_CountMode_Route53Customer;
    private final String serviceName = ServiceName.Route53;
    private final String userName = "TestUserName";
    private final String toolName = "TestToolName";
    private final String description = "TestDesc";
    
    private RequestValidator validator = new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class)),
            mock(EdgeLocationsHelper.class),
            mock(BlackWatchBorderLocationValidator.class));
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        validator = new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class)),
            mock(EdgeLocationsHelper.class),
            mock(BlackWatchBorderLocationValidator.class));
    }
    
    /**
     * Test the case where the request is completely valid. We expect the validation to be completed without any exceptions.
     */
    @Test
    public void testHappyCase() {
        // RateLimit MitigationTemplate
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        validator.validateCreateRequest(request);
        
        // CountMode MitigationTemplate
        request.setMitigationTemplate(rateLimitMitigationTemplate);
        validator.validateCreateRequest(request);
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationName() {
        // RateLimit MitigationTemplate
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
        
        // CountMode MitigationTemplate
        request.setMitigationTemplate(countModeMitigationTemplate);
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request has invalid mitigationNames (empty, only spaces, special characters (non-printable ascii). 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testInvalidMitigationNames() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("");
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
        
        // Check mitigationNames with all spaces
        request.setMitigationName("   ");
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
        
        // Check mitigationNames with non-printable ascii characters.
        char invalidChar = 0x00;
        request.setMitigationName("Some name with invalid char: " + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
        
        // Check mitigationNames with delete non-printable ascii character.
        invalidChar = 0x7F;
        request.setMitigationName("Some name with delete char: " + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
        
        // Check extra long mitigationNames.
        StringBuffer buffer = new StringBuffer();
        for (int index=0; index < 500; ++index) {
            buffer.append("a");
        }
        request.setMitigationName(buffer.toString());
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation name"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the mitigationTemplate
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingOrInvalidRateLimitMitigationTemplate() {
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().startsWith("Invalid mitigation template found"));
        
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
        
        // Check mitigationTemplate with non-printable ascii characters.
        char invalidChar = 0x00;
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid mitigation template"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the serviceName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingOrInvalidServiceName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
        
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid service name found"));
        
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
        
        // Check serviceName with non-printable ascii characters.
        char invalidChar = 0x00;
        request.setServiceName(ServiceName.Route53 + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid service name"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the mitigationDefinition
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationActionMetadata() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
        request.setServiceName(serviceName);
        
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
        constraint.setAttributeValues(Lists.newArrayList("1.2.3.4"));
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        request.setMitigationDefinition(definition);
        
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
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid user name"));
        
        // Check userName with non-printable ascii characters.
        char invalidChar = 0x00;
        metadata.setUser("Test User: " + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid user name"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingToolName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid tool name"));
        
        // Check toolName with non-printable ascii characters.
        char invalidChar = 0x00;
        metadata.setToolName("Tool: " + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid tool name"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationDescription() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
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
        
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid description found"));
        
        // Check description with non-printable ascii characters.
        char invalidChar = 0x00;
        metadata.setDescription("Description: " + String.valueOf(invalidChar));
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid description found"));
        }
        assertNotNull(caughtException);
        
        // Check extra long description.
        StringBuffer buffer = new StringBuffer();
        for (int index=0; index < 1000; ++index) {
            buffer.append("a");
        }
        metadata.setDescription("Description: " + buffer);
        caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Invalid description found"));
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the happy case for a delete request. We don't expect any exception to be thrown in this case.
     */
    @Test
    public void testValidateDeleteRequestHappyCase() {
        // RateLimit MitigationTemplate
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
        request.setServiceName(serviceName);
        request.setMitigationVersion(2);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        request.setMitigationActionMetadata(metadata);
        
        validator.validateDeleteRequest(request);
        
        // CountMode MitigationTemplate
        request.setMitigationTemplate(countModeMitigationTemplate);
        validator.validateDeleteRequest(request);       
    }
    
    /**
     * Test the case for a delete request with no mitigation version provided as input. 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testDeleteRequestWithInvalidMitigationVersion() {
        // RateLimit MitigationTemplate
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(rateLimitMitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        request.setMitigationActionMetadata(metadata);
        
        Throwable caughtException = null;
        try {
            validator.validateDeleteRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Version of the mitigation to be deleted should be set to >=1"));
        }
        assertNotNull(caughtException);        
        
        // CountMode MitigationTemplate
        request.setMitigationTemplate(countModeMitigationTemplate);
        caughtException = null;
        try {
            validator.validateDeleteRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(ex.getMessage().startsWith("Version of the mitigation to be deleted should be set to >=1"));
        }
    }
    
    /**
     * Test the case for a create request with duplicates in the related tickets. 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testCreateRequestWithDuplicateRelatedTickets() {
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest(
                rateLimitMitigationTemplate, mitigationName, serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        metadata.setRelatedTickets(Lists.newArrayList("Tkt1", "Tkt2", "Tkt2"));
        request.setMitigationActionMetadata(metadata);
        
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
    
    /* 
     * Test to ensure the number of tickets is restricted.
     */
    @Test
    public void testCreateRequestWithInvalidRelatedTickets() {
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest(
                rateLimitMitigationTemplate, mitigationName, serviceName);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        metadata.setRelatedTickets(Lists.newArrayList("00123456789"));
        request.setMitigationActionMetadata(metadata);
        
        List<String> relatedTickets = Lists.newArrayList();
        for (int index=0; index < 200; ++index) {
            relatedTickets.add("000001" + index);
        }
        metadata.setRelatedTickets(relatedTickets);
        Throwable caughtException = null;
        try {
            validator.validateCreateRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().startsWith("Exceeded the number of tickets that can be specified for a single mitigation"));
    }
    
    @Test
    public void testListActiveMitigationsForService() {        
        ListActiveMitigationsForServiceRequest request = new ListActiveMitigationsForServiceRequest();
        
        // locations is optional
        request.setServiceName(serviceName);
        request.setDeviceName(DeviceName.POP_ROUTER.name());
        validator.validateListActiveMitigationsForServiceRequest(request);
        
        // deviceName may not be a random name.
        Throwable caughtException = null;
        request.setDeviceName("arbit");
        try {
            validator.validateListActiveMitigationsForServiceRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        // valid device name
        request.setDeviceName("POP_ROUTER");
        validator.validateListActiveMitigationsForServiceRequest(request);
        
        // locations if set may not be empty
        caughtException = null;
        request.setLocations(new ArrayList<String>());
        try {
            validator.validateListActiveMitigationsForServiceRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(caughtException.getMessage().startsWith("Empty list of locations found"));
        }
        assertNotNull(caughtException);
        
        // locations may not be empty
        caughtException = null;
        request.setLocations(Arrays.asList(""));
        try {
            validator.validateListActiveMitigationsForServiceRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(caughtException.getMessage().startsWith("Invalid location name"));
        }
        assertNotNull(caughtException);
        
        // Test to ensure the number of locations is restricted.
        List<String> locations = new ArrayList<>();
        for (int index=0; index < 500; ++index) {
            locations.add("TST" + index);
        }
        request.setLocations(locations);
        
        caughtException = null;
        try {
            validator.validateListActiveMitigationsForServiceRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(caughtException.getMessage().startsWith("Exceeded the number of locations that can be specified for a single request"));
        }
        assertNotNull(caughtException);
        
        request.setLocations(Arrays.asList("alocation"));
        caughtException = null;
        try {
            validator.validateListActiveMitigationsForServiceRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
            assertTrue(caughtException.getMessage().startsWith("Invalid location name"));
        }
        assertNotNull(caughtException);
        
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        validator = new RequestValidator(new ServiceLocationsHelper(edgeLocationsHelper), mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class));
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("alocation", "blocation", "clocations"));
        validator.validateListActiveMitigationsForServiceRequest(request);
    }
    
    @Test
    public void testReportInactiveLocationActivity() {        
        ReportInactiveLocationRequest request = new ReportInactiveLocationRequest();
        
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        validator = new RequestValidator(new ServiceLocationsHelper(edgeLocationsHelper), mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class));
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("alocation", "blocation", "clocations"));
        
        // locations is optional
        request.setServiceName(serviceName);
        request.setDeviceName(DeviceName.POP_ROUTER.name());
        request.setLocation("alocation");
        
        Throwable caughtException = null;
        try {
            validator.validateReportInactiveLocation(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        // invalid device name
        request.setDeviceName("random");
        caughtException = null;
        try {
            validator.validateReportInactiveLocation(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        // invalid service name
        request.setDeviceName(DeviceName.POP_ROUTER.name());
        request.setServiceName("random");
        caughtException = null;
        try {
            validator.validateReportInactiveLocation(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        // invalid location
        request.setServiceName(serviceName);
        request.setLocation("random");
        caughtException = null;
        try {
            validator.validateReportInactiveLocation(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
}
