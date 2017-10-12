package com.amazon.lookout.mitigation.service.activity.validator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig.MitigationAction;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.test.common.util.TestUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


public class RequestValidatorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    private final String mitigationName = "TestMitigationName";
    private final String template1 = MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer;
    private final String template2 = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    private final String serviceName = ServiceName.Edge;
    private final String userName = "TestUserName";
    private final String toolName = "TestToolName";
    private final String description = "TestDesc";
    private static final String locationConfigFilePath = 
            System.getProperty("user.dir") + "/tst-data/test_mitigation_service_locations.json";

    private RequestValidator validator = new RequestValidator(
            mock(EdgeLocationsHelper.class),
            mock(BlackWatchBorderLocationValidator.class),
            mock(BlackWatchEdgeLocationValidator.class),
            locationConfigFilePath);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        validator = new RequestValidator(
            mock(EdgeLocationsHelper.class),
            mock(BlackWatchBorderLocationValidator.class),
            mock(BlackWatchEdgeLocationValidator.class),
            locationConfigFilePath);
    }
    

    @Test
    public void testValidateAbortDeploymentRequest() {

    	String[] validBWTemplates = {MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer, MitigationTemplate.BlackWatchPOP_EdgeCustomer, MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer};
    	AbortDeploymentRequest abortRequest = new AbortDeploymentRequest();
    	abortRequest.setJobId(1);
    	abortRequest.setServiceName(ServiceName.Edge);
    	abortRequest.setDeviceName(DeviceName.BLACKWATCH_POP.name());
    	//valid template
    	for (String template : validBWTemplates) {
    		abortRequest.setMitigationTemplate(template);
    		validator.validateAbortDeploymentRequest(abortRequest);
    	}
    }
    
    /**
     * Test the case where the request is completely valid. We expect the validation to be completed without any exceptions.
     */
    @Test
    public void testHappyCase() {
        // RateLimit MitigationTemplate
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template2);
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
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer + String.valueOf(invalidChar));
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
        request.setMitigationTemplate(template1);
        
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
        request.setServiceName(ServiceName.Edge + String.valueOf(invalidChar));
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
        request.setMitigationTemplate(template1);
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
    }
    
    /**
     * Test the case where the request is missing the mitigationName 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingUserName() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template1);
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
        request.setMitigationTemplate(template1);
        request.setServiceName(serviceName);
        request.setMitigationVersion(2);

        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser(userName);
        metadata.setToolName(toolName);
        metadata.setDescription("Test description");
        request.setMitigationActionMetadata(metadata);
        
        validator.validateDeleteRequest(request);
        
        // CountMode MitigationTemplate
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
        request.setMitigationTemplate(template1);
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
    }
    
    /**
     * Test the case for a create request with duplicates in the related tickets. 
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testCreateRequestWithDuplicateRelatedTickets() {
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest(
                template1, mitigationName, serviceName);
        
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
                template1, mitigationName, serviceName);
        
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
        request.setDeviceName(DeviceName.BLACKWATCH_POP.name());
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
        request.setDeviceName("BLACKWATCH_POP");
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

        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        request.setLocations(Arrays.asList("alocation"));
        validator = new RequestValidator(mock(EdgeLocationsHelper.class),
                mock(BlackWatchBorderLocationValidator.class),
                mock(BlackWatchEdgeLocationValidator.class),
                locationConfigFilePath);
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("alocation", "blocation", "clocations"));
        validator.validateListActiveMitigationsForServiceRequest(request);
    }
    
    @Test
    public void testvalidateListBlackWatchMitigationsRequest() {
        ListBlackWatchMitigationsRequest request = new ListBlackWatchMitigationsRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Khaleesi").withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655")).build());
        
        //valid request with only the MitigationActionMetadata, all other fields are null.
        validator.validateListBlackWatchMitigationsRequest(request);

        String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        String validResourceId = "192.168.0.1";
        String validResourceType = "IPAddress";
        String validOwnerARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        //valid mitigationid, resourceid, resourcetype, ownerarn.
        request.setMitigationId(validMitigationId);
        request.setResourceId(validResourceId);
        request.setResourceType(validResourceType);
        request.setOwnerARN(validOwnerARN);
        validator.validateListBlackWatchMitigationsRequest(request);
        request.setMaxResults(5L);
        validator.validateListBlackWatchMitigationsRequest(request);

        //Invalid mitigationId;
        Throwable caughtException = null;
        char invalidChar = 0x00;
        request.setMitigationId(validMitigationId + String.valueOf(invalidChar));
        try {
            validator.validateListBlackWatchMitigationsRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid mitigation ID"));
        request.setMitigationId(validMitigationId);
        
        //invalid resource id
        caughtException = null;
        request.setResourceId(validResourceId + String.valueOf(invalidChar));
        try {
            validator.validateListBlackWatchMitigationsRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid resource ID"));
        request.setResourceId(validResourceId);

        //invalid resource type
        caughtException = null;
        request.setResourceType(validResourceType + String.valueOf(invalidChar));
        try {
            validator.validateListBlackWatchMitigationsRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid resource type"));
        request.setResourceType(validResourceType);
        
        //invalid user ARN
        caughtException = null;
        request.setOwnerARN(validOwnerARN + String.valueOf(invalidChar));
        try {
            validator.validateListBlackWatchMitigationsRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid user ARN"));
        request.setOwnerARN(validOwnerARN);
        
        //invalid max result
        caughtException = null;
        request.setMaxResults(0L);
        try {
            validator.validateListBlackWatchMitigationsRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid maxNumberOfEntriesToFetch"));        
    }
    
    @Test
    public void testvalidateDeactivateBlackWatchMitigationRequest() {
        DeactivateBlackWatchMitigationRequest request = new DeactivateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Khaleesi").withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655")).build());

        Throwable caughtException = null;
        
        //invalid request with only the MitigationActionMetadata, all other fields are null.
        try {
            validator.validateDeactivateBlackWatchMitigationRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
 
        String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        //valid mitigationid
        request.setMitigationId(validMitigationId);
        validator.validateDeactivateBlackWatchMitigationRequest(request);
 
        //Invalid mitigationId;
        char invalidChar = 0x00;
        request.setMitigationId(validMitigationId + String.valueOf(invalidChar));
        try {
            validator.validateDeactivateBlackWatchMitigationRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid mitigation ID"));
        request.setMitigationId(validMitigationId);
        
    }
    
    @Test
    public void testApplyMitigationRequestValidator() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("Dragons")
                .withDescription("MOD butt kicking.")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());
        char invalidChar = 0x00;
        
        Throwable caughtExcepion = null;
        String userARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid resource ID"));
        caughtExcepion = null;
        
        String resourceId = "IPList1234";
        request.setResourceId(resourceId);
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid resource type"));
        caughtExcepion = null;
        
        String resourceType = "IPAddressXYZZZZ";
        request.setResourceType(resourceType);
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Unsupported resource type"));
        caughtExcepion = null;
        
        resourceType = "IPAddressList";
        request.setResourceType(resourceType);
        
        String JSON = "XHHSHS";
        request.setMitigationSettingsJSON(JSON);
        
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Could not parse"));
        caughtExcepion = null;

        //Allow null for the JSON.
        request.setMitigationSettingsJSON(null);

        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("A default rate limit must"));
        caughtExcepion = null;

        // Specify a global rate limit.
        request.setGlobalPPS(5L);
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);

        JSON="{\"ipv6_per_dest_depth\":56, \"mitigation_config\":{}}";
        request.setMitigationSettingsJSON(JSON);
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, "ABABA" + String.valueOf(invalidChar));
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid user ARN"));
        caughtExcepion = null;
        
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, "");
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid user ARN"));
        caughtExcepion = null;
        
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, null);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid user ARN"));
        caughtExcepion = null;

        request.getMitigationActionMetadata().setUser("");
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid user"));
        caughtExcepion = null;
        request.getMitigationActionMetadata().setUser("Valid");

        request.getMitigationActionMetadata().setToolName("");
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid tool"));
        caughtExcepion = null;
        request.getMitigationActionMetadata().setToolName("Towel");

        request.getMitigationActionMetadata().setRelatedTickets(Arrays.asList("1234", "1234"));
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage().startsWith("Duplicate related tickets"));
        caughtExcepion = null;
    }

    private Long getDefaultRateLimit(final BlackWatchTargetConfig targetConfig) {
        return targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_pps();
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationWithPPS() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 42L;
        request.setGlobalPPS(theRateLimit);

        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit, getDefaultRateLimit(newTargetConfig));
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationWithJson() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 43L;

        String json = String.format(""
            + "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": %d"
            + "      }"
            + "    }"
            + "  }"
            + "}", theRateLimit);
        request.setMitigationSettingsJSON(json);

        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit, getDefaultRateLimit(newTargetConfig));
    }

    @Test
    public void testChangeOwnerARNRequestvalidation() {
        ChangeBlackWatchMitigationOwnerARNRequest request = new ChangeBlackWatchMitigationOwnerARNRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());

        Throwable caughtException = null;
        
        //invalid request with only the MitigationActionMetadata, all other fields are null.
        try {
            validator.validateChangeBlackWatchMitigationOwnerARNRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        caughtException = null;
 
        String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        String validOwnerARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        char invalidChar = 0x00;

        //valid 
        request.setMitigationId(validMitigationId);
        request.setExpectedOwnerARN(validOwnerARN);
        request.setNewOwnerARN(validOwnerARN);
        validator.validateChangeBlackWatchMitigationOwnerARNRequest(request);
 
        //Invalid mitigationId;
        request.setMitigationId(validMitigationId + String.valueOf(invalidChar));
        try {
            validator.validateChangeBlackWatchMitigationOwnerARNRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
        assertTrue(caughtException.getMessage().startsWith("Invalid mitigation ID"));
        request.setMitigationId(validMitigationId);
        
        //Invalid newOwnerARN;
        request.setNewOwnerARN(validOwnerARN + String.valueOf(invalidChar));
        try {
            validator.validateChangeBlackWatchMitigationOwnerARNRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        caughtException = null;
        
        request.setNewOwnerARN(validOwnerARN);
        
        //Invalid expectedOwnerARN;
        request.setExpectedOwnerARN(validOwnerARN + String.valueOf(invalidChar));
        try {
            validator.validateChangeBlackWatchMitigationOwnerARNRequest(request);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testValidateListBlackWatchLocationsRequest() {
        ListBlackWatchLocationsRequest request = new ListBlackWatchLocationsRequest();

        // valid region name
        request.setRegion("valid-region-1");
        validator.validateListBlackWatchLocationsRequest(request);

        // region name is optional, so valid input
        request.setRegion(null);
        validator.validateListBlackWatchLocationsRequest(request);

        // invalid region name
        request.setRegion("not-valid-region-name");
        Throwable caughtException = null;
        try {
            validator.validateListBlackWatchLocationsRequest(request);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testValidateMitigationSettingsEmptyJSON() {
        // Empty object is fine.
        String json = "{}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);  // A default rate limit is required
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsEmptyString() {
        // Empty string is fine
        String json = "";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);  // A default rate limit is required
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsNull() {
        // Null value is fine
        String json = null;
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);  // A default rate limit is required
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsNotJSON() {
        // Invalid JSON should fail to parse
        String json = "not json";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsUnknownKey() {
        // Valid JSON not conforming to the model should fail.
        String json = "{ \"unknown_key\": true }";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
    }
    
    @Test
    public void testValidateMitigationSettingsValidJSON() {
        // Valid JSON matching the model should pass.
        String json = "{ \"mitigation_config\": { \"ip_validation\": { \"action\": \"DROP\" } } }";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);  // A default rate limit is required
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getIp_validation());
        assertSame(MitigationAction.DROP, targetConfig.getMitigation_config().getIp_validation().getAction());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsValidIpTrafficShaper() {
        // JSON specifying valid ip_traffic_shaper is not allowed - must use global_traffic_shaper
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"ip_traffic_shaper\": {"
            + "    \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"enable_per_host\": false,"
            + "        \"default\": {"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }
    
    @Test
    public void testValidateMitigationSettingsGlobalTrafficShaper() {
        // JSON specifying valid global_traffic_shaper is fine
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getGlobal_traffic_shaper());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsGlobalTrafficShaperMissingPPS() {
        // global_traffic_shaper without pps should be rejected
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsGlobalTrafficShaperNegativePPS() {
        // global_traffic_shaper with negative pps should be rejected
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": -10,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsGlobalTrafficShaperDuplicate() {
        // global_traffic_shaper with duplicate shaper names should be rejected
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 10,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"default\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsBothGlobalAndIpTrafficShaper() {
        // Can't specify both ip_traffic_shaper and global_traffic_shaper
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"ip_traffic_shaper\": {"
            + "    \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"enable_per_host\": false,"
            + "        \"default\": {"
            + "        }"
            + "      }"
            + "    },"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"blobal_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidShaperActions() {
        // global_traffic_shaper with allowed actions
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"action\": \"DROP\","
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"action\": \"COUNT\","
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      },"
            + "      \"named_two\": {"
            + "        \"action\": \"PASS\","
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getGlobal_traffic_shaper());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testInvalidShaperAction() {
        // RESPOND action is not valid for shapers
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"action\": \"RESPOND\","
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testUnknownShaperAction() {
        // Shaper with an unrecognized action
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"action\": \"NANANANANA-BATMAN\","
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testMergeNothing() {
        // Test that the target config is not changed by merging null values
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        assertNull(targetConfig.getMitigation_config());
        validator.mergeGlobalPpsBps(targetConfig, null, null);
        assertNull(targetConfig.getMitigation_config());
    }

    @Test
    public void testMergePpsEmptyTargetConfig() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);

        // Wrong value or NullPointerException both mean that the value was not
        // stored correctly
        Long globalPps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_pps();
        assertSame(5L, globalPps);
    }

    @Test
    public void testMergeBpsEmptyTargetConfig() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        validator.mergeGlobalPpsBps(targetConfig, null, 5L);

        // Wrong value or NullPointerException both mean that the value was not
        // stored correctly
        Long globalBps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_bps();
        assertSame(5L, globalBps);
    }

    @Test
    public void testMergePpsBpsEmptyTargetConfig() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        validator.mergeGlobalPpsBps(targetConfig, 10L, 5L);

        // Wrong value or NullPointerException both mean that the value was not
        // stored correctly
        Long globalPps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_pps();
        assertSame(10L, globalPps);

        Long globalBps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_bps();
        assertSame(5L, globalBps);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMergeDuplicatePps() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();

        // Add a pps value to the target config
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);

        // Adding another value causes validation to fail
        validator.mergeGlobalPpsBps(targetConfig, 6L, null);
    }

    // Test that the "merge ignoring duplicate rates" works and that the new
    // rate overrides the old one
    @Test
    public void testMergeDuplicatePpsIgnoreDuplicate() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();

        Long oldRate = 5L;
        Long newRate = 6L;

        // Add a pps value to the target config
        validator.mergeGlobalPpsBps(targetConfig, oldRate, null, false);

        // Adding another value does not raise an exception
        validator.mergeGlobalPpsBps(targetConfig, newRate, null, false);

        // Validate that the new value overrides the old one
        Long globalPps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_pps();
        assertSame(newRate, globalPps);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMergeDuplicateBps() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();

        // Add a pps value to the target config
        validator.mergeGlobalPpsBps(targetConfig, null, 5L);

        // Adding another value causes validation to fail
        validator.mergeGlobalPpsBps(targetConfig, null, 6L);
    }

    // Test that the "merge ignoring duplicate rates" works and that the new
    // rate overrides the old one
    @Test
    public void testMergeDuplicateBpsIgnoreDuplicate() {
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();

        Long oldRate = 5L;
        Long newRate = 6L;

        // Add a pps value to the target config
        validator.mergeGlobalPpsBps(targetConfig, null, oldRate, false);

        // Adding another value does not raise an exception
        validator.mergeGlobalPpsBps(targetConfig, null, newRate, false);

        // Validate that the new value overrides the old one
        Long globalBps = targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_bps();
        assertSame(newRate, globalBps);
    }

    @Test
    public void testMergeOneParameterOneJson() {
        // Merge a PPS once then a BPS once.  This simulates what happens when
        // a user specifies a PPS via the API parameter and BPS via JSON.  There
        // is no real reason to ever do this, but it's not an error.
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        validator.mergeGlobalPpsBps(targetConfig, null, 5L);
        validator.mergeGlobalPpsBps(targetConfig, 6L, null);

        // Other way around
        targetConfig = new BlackWatchTargetConfig();
        validator.mergeGlobalPpsBps(targetConfig, 5L, null);
        validator.mergeGlobalPpsBps(targetConfig, null, 6L);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDefaultRateLimitNotSpecified() {
        // A well-formed request, but it does not specify a default rate limit anywhere.
        // Expect validation to fail.
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");
        request.setMitigationSettingsJSON("{}");
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testDefaultRateLimitSpecifiedInAPIField() {
        // The default rate limit is specified by the GlobalPPS API parameter
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");
        request.setGlobalPPS(1000L);
        request.setMitigationSettingsJSON("{}");
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testDefaultRateLimitSpecifiedInJSON() {
        // The default rate limit is specified by the JSON global_traffic_shaper key
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");
        String json = "{\"mitigation_config\": {\"global_traffic_shaper\": {\"default\": {\"global_pps\": 1000}}}}";
        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDefaultRateLimitSpecifiedInAPIFieldAndJSON() {
        // The default rate limit is specified by both the GlobalPPS API parameter
        // and the JSON global_traffic_shaper field
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");
        request.setGlobalPPS(1000L);
        String json = "{\"mitigation_config\": {\"global_traffic_shaper\": {\"default\": {\"global_pps\": 1000}}}}";
        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNamedRateLimitSpecifiedInJSON() {
        // A named shaper rate limit is specified by the JSON global_traffic_shaper key,
        // but the default rate isn't specified anywhere
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");
        String json = "{\"mitigation_config\": {\"global_traffic_shaper\": {\"name1\": {\"global_pps\": 1000}}}}";
        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testZeroRateLimitSpecifiedInJSON() {
        // We tolerate a rate limit of zero when only the default shaper is present.
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");

        String json = ""
            + "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 0"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNamedZeroRateLimitSpecifiedInJSON() {
        // A zero rate limit for any other shaper should be rejected
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");

        String json = ""
            + "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"not_default\": {"
            + "        \"global_pps\": 0"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNamedDefaultZeroRateLimitSpecifiedInJSON() {
        // A zero rate limit for the default shaper is rejected when more than one
        // shaper exist.
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddressList");

        String json = ""
            + "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 0"
            + "      },"
            + "      \"not_default\": {"
            + "        \"global_pps\": 1000"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testValidateMitigationSettingsNetworkACL() {
        // test valid network ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"fragments_rules\": [],"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig().getRules());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig().getFragments_rules());
    }

    @Test
    public void testValidateMitigationSettingsFragmentNetworkACL() {
        // test valid fragment network ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [],"
            + "        \"fragments_rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig().getRules());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig().getFragments_rules());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidNetworkACL_1() {
        // both src and src_country field are specified in ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidFragmentNetworkACL_1() {
        // both src and src_country field are specified in fragment ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [],"
            + "        \"fragments_rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidNetworkACL_2() {
        // src and src_country field is missing in ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidFragmentNetworkACL_2() {
        // src and src_country field is missing in fragments ACL rule
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [],"
            + "        \"fragments_rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidNetworkACL_3() {
        // rules field is missing in network_acl
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"fragments_rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsValidNetworkACL() {
        // fragments_rules field is missing in network_acl
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl());
        assertNotNull(targetConfig.getMitigation_config().getNetwork_acl().getConfig());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidNetworkACL_4() {
        // both fragment_rules and rules fields are missing in network_acl
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\""
            + "      \"config\": {"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsValidSuspicionScore_1() {
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"baseline_fractions\": {"
            + "          \"BA_DNS_ANY_QUERY\": 2.40454500034E-4,"
            + "          \"BA_DNS_BAD_ANSWER_COUNT\": 1.34885045647E-10,"
            + "          \"BA_DNS_BAD_EDNS0_NAME\": 1.05123052372E-9"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score().getConfig());
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsValidSuspicionScore_2() {
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"@inherit\": \"blackwatch.suspicion_score.global.config.default\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score().getConfig());
        validator.validateTargetConfig(targetConfig);
    }

    @Test
    public void testValidateMitigationSettingsValidSuspicionScore_3() {
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"@inherit\": \"blackwatch.suspicion_score.baseline.config.default\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        assertNotNull(targetConfig);
        assertNotNull(targetConfig.getMitigation_config());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score());
        assertNotNull(targetConfig.getMitigation_config().getSuspicion_score().getConfig());
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidSuspicionScore_1() {
        // empty config
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidSuspicionScore_2() {
        // includes both a baseline_fractions and @inherit macro
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"baseline_fractions\": {"
            + "          \"BA_DNS_ANY_QUERY\": 2.40454500034E-4,"
            + "          \"BA_DNS_BAD_ANSWER_COUNT\": 1.34885045647E-10,"
            + "          \"BA_DNS_BAD_EDNS0_NAME\": 1.05123052372E-9"
            + "        },"
            + "        \"@inherit\": \"blackwatch.suspicion_score.global.config.default\""
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testValidateMitigationSettingsInvalidSuspicionScore_3() {
        // @inherit macro is not in right format
        String json = "{"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      },"
            + "      \"named_one\": {"
            + "        \"global_bps\": 0,"
            + "        \"global_pps\": 12"
            + "      }"
            + "    },"
            + "    \"network_acl\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"rules\": [{"
            + "          \"action\": \"DENY\","
            + "          \"dst\": \"0.0.0.0/0\","
            + "          \"src_country\": \"AU\","
            + "          \"name\": \"RANDOM_ACL_RULE_NAME\","
            + "          \"l4_proto\": {"
            + "            \"value\": 6"
            + "          }"
            + "        }]"
            + "      }"
            + "    },"
            + "    \"suspicion_score\": {"
            + "      \"action\": \"DROP\","
            + "      \"config\": {"
            + "        \"@inherit\": \"blahblahblah\""
            + "      }"
            + "    }"
            + "  }"
            + "}";
        BlackWatchTargetConfig targetConfig = validator.parseMitigationSettingsJSON(json);
        validator.validateTargetConfig(targetConfig);
    }
}

