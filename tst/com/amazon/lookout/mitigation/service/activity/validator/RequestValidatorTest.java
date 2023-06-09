package com.amazon.lookout.mitigation.service.activity.validator;

import static com.amazon.lookout.mitigation.service.activity.validator.RequestValidator.REGIONAL_CELL_NAME_PATTERN;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
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
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.test.common.util.TestUtils;
import com.google.common.collect.Lists;
import org.junit.rules.ExpectedException;


public class RequestValidatorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    private final String mitigationName = "TestMitigationName";
    private final String template1 = MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer;
    private final String template2 = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    private final String userName = "TestUserName";
    private final String toolName = "TestToolName";
    private final String description = "TestDesc";
    private static final String currentRegion = "test-region";
    private BwirCellConfig bwirCellConfig =
            spy(new BwirCellConfig(System.getProperty("user.dir") + "/build/private/etc/cells/"));

    private RequestValidator validator = new RequestValidator(currentRegion, bwirCellConfig);

    @Rule
    public ExpectedException invalidRegion = ExpectedException.none();

    @Rule
    public ExpectedException invalidCellNamePattern = ExpectedException.none();

    @Rule
    public ExpectedException invalidCellName = ExpectedException.none();

    @Rule
    public ExpectedException notYetLaunchedCells = ExpectedException.none();
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    

    @Test
    public void testValidateAbortDeploymentRequest() {
        String[] validBWTemplates = {
                MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer,
                MitigationTemplate.BlackWatchPOP_EdgeCustomer,
                MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer
        };
        AbortDeploymentRequest abortRequest = new AbortDeploymentRequest();
        abortRequest.setJobId(1);
        abortRequest.setDeviceName(DeviceName.BLACKWATCH_POP.name());
        // valid template
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
     * Test the case where the request is missing the mitigationDefinition
     * We expect an exception to be thrown in this case.
     */
    @Test
    public void testMissingMitigationActionMetadata() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(template1);
        
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
                template1, mitigationName);
        
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
                template1, mitigationName);
        
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
        request.setDeviceName(DeviceName.BLACKWATCH_POP.name());
        validator.validateListActiveMitigationsForServiceRequest(request);
        
        // valid device name
        request.setDeviceName("BLACKWATCH_POP");
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
        assertTrue(caughtExcepion.getMessage().startsWith("Invalid resource type"));
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
        request.setGlobalPPS(120L);
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

        final Long theRateLimit = 120L;
        request.setGlobalPPS(theRateLimit);

        String resouceType = "IPAddress";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit, getDefaultRateLimit(newTargetConfig));
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationRegionalCellPlacementWithValidCellNames() {
        UpdateBlackWatchMitigationRegionalCellPlacementRequest request =
                new UpdateBlackWatchMitigationRegionalCellPlacementRequest();
        List<String> cellNames = Stream.of("bzg-pdx-c1", "BZG-PDX-C2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        validator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request, "gamma", "us-west-2");
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationRegionalCellPlacementWithInvalidCellNamePattern() {
        UpdateBlackWatchMitigationRegionalCellPlacementRequest request =
                new UpdateBlackWatchMitigationRegionalCellPlacementRequest();
        List<String> cellNames = Stream.of("bzg-pdx-c1", "bz-pdx1-c2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        invalidCellNamePattern.expect(IllegalArgumentException.class);
        invalidCellNamePattern.expectMessage(String.format(
                "Unrecognized cellName pattern found: 'bz-pdx1-c2', expected pattern is: '%s'.", REGIONAL_CELL_NAME_PATTERN));
        validator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request, "gamma", "us-west-2");
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationRegionalCellPlacementWithCellNameNotInCellConfig() {
        UpdateBlackWatchMitigationRegionalCellPlacementRequest request =
                new UpdateBlackWatchMitigationRegionalCellPlacementRequest();
        List<String> cellNames = Stream.of("bzg-pdx-c1", "BZG-XYZ-C2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        invalidCellName.expect(IllegalArgumentException.class);
        invalidCellName.expectMessage("cellName: 'BZG-XYZ-C2' not found in Bwir cell config");
        validator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request, "gamma", "us-west-2");
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationRegionalCellPlacementRequestOnNotYetLaunchedCells() {
        UpdateBlackWatchMitigationRegionalCellPlacementRequest request =
                new UpdateBlackWatchMitigationRegionalCellPlacementRequest();
        List<String> cellNames = Stream.of("bza-pdx-c1", "BZA-PDX-C2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        String domain = "alpha";
        String realm = "us-west-2";

        notYetLaunchedCells.expect(IllegalArgumentException.class);
        notYetLaunchedCells.expectMessage(String.format("Domain: %s and Realm: %s doesn't contain "
                        + "any BWIR cells yet, but UpdateCellPlacementRequest contains cells: %s",
                domain, realm, cellNames));
        validator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request, domain, realm);
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationRegionalCellPlacementWithSpecialDomainCase() {
        UpdateBlackWatchMitigationRegionalCellPlacementRequest request =
                new UpdateBlackWatchMitigationRegionalCellPlacementRequest();
        List<String> cellNames = Stream.of("bzg-pdx-c1", "bzg-pdx-c2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        validator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request, "gamma-border", "us-west-2");
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

        final Long theRateLimit = 120L;

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

        String resouceType = "IPAddress";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationWithInvalidJsonBypassValidations() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);
        request.setBypassConfigValidations(true);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 0L;

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

        String resouceType = "IPAddress";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

    @Test
    public void testValidateUpdateBlackWatchMitigationWithEmptyJsonForGLB() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        String json = "{}";
        request.setMitigationSettingsJSON(json);

        String resouceType = "GLB";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
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
        request.setRegion("test-region");
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

    // similar test as testNamedDefaultZeroRateLimitSpecifiedInJSON but since
    // bypass validation is set, the test should succeed.
    @Test
    public void testNamedDefaultZeroRateLimitSpecifiedInJSONBypassValidation() {
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
        request.setBypassConfigValidations(true);

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
    public void testInvalidIPv4ResourceIdIpAddressListMitigation() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(
                                            MitigationActionMetadata.builder().withUser("Username")
                                                                    .withToolName("Toolname")
                                                                    .withDescription("Description")
                                                                    .withRelatedTickets(Arrays.asList("1234", "5655"))
                                                                    .build());
        request.setResourceId("10.0.12.0/24");
        request.setResourceType("IPAddressList");

        Throwable caughtExcepion = null;
        String userARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                                 .startsWith("Invalid resource ID! An IP Address List resource ID cannot be a Network CIDR"));
    }
    
    @Test
    public void testInvalidIPv6ResourceIdIpAddressListMitigation() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(
                                            MitigationActionMetadata.builder().withUser("Username")
                                                                    .withToolName("Toolname")
                                                                    .withDescription("Description")
                                                                    .withRelatedTickets(Arrays.asList("1234", "5655"))
                                                                    .build());
        request.setResourceId("2001:1111:2222:3333::/64");
        request.setResourceType("IPAddressList");

        Throwable caughtExcepion = null;
        String userARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                                 .startsWith("Invalid resource ID! An IP Address List resource ID cannot be a Network CIDR"));
    }

    /**
     * Test the case where the request resourceId contains an ascii-printable whitespace.
     */
    @Test
    public void testInvalidWhitespaceResourceIdIpAddressListMitigation() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(
                                            MitigationActionMetadata.builder().withUser("Username")
                                                                    .withToolName("Toolname")
                                                                    .withDescription("Description")
                                                                    .withRelatedTickets(Arrays.asList("1234", "5655"))
                                                                    .build());
        request.setResourceId("ResourceId 1 2 3 ");
        request.setResourceType("IPAddressList");

        Throwable caughtExcepion = null;
        String userARN = "arn:aws:iam::005436146250:user/blackwatch_host_status_updator_blackwatch_pop_pro";
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                                 .startsWith("Invalid resource ID! An IP Address List resource ID cannot contain whitespace characters"));
    }

    @Test
    public void testGlbRequestsAreValidated() {
        invalidRegion.expect(IllegalArgumentException.class);
        invalidRegion.expectMessage("Key: no-such-1 must be a region in the aws partition");

        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder().withUser("Username")
                        .withToolName("Toolname")
                        .withDescription("Description")
                        .withRelatedTickets(Arrays.asList("1234", "5655"))
                        .build());
        request.setResourceId("AGlbArn");
        request.setResourceType("GLB");

        String json = ""
                + "{" +
                "  \"glb_mitigation_config\": {\n" +
                "    \"listener_configs\": {\n" +
                "      \"default\": {\n" +
                "        \"regional_capacity_limits\": {\n" +
                "          \"no-such-1\": {\n" + // this should cause the validate call, if made, to throw an exception
                "            \"global_pps\": 330000\n" +
                "          },\n" +
                "          \"eu-west-2\": {\n" +
                "            \"global_pps\": 440000\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n"+
                "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }


    // Test BWiR resource type validation
    @Test
    public void testValidApplyBlackWatchMitigationBWiRIPAddress() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("IPAddress");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testValidApplyBlackWatchMitigationBWiRIPAddressList() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("IPAdResourceId123");
        request.setResourceType("IPAddressList");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\", \"GLOBAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testValidApplyBlackWatchMitigationBWiRElasticIP() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("ElasticIP");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testInvalidApplyBlackWatchMitigationBWiRELB() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("ELB");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        Throwable caughtExcepion = null;
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                .startsWith("Unsupported resource type specified for BWiR"));
    }

    @Test
    public void testInvalidApplyBlackWatchMitigationBWiRGLB() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("GLB");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        Throwable caughtExcepion = null;
        try {
            validator.validateApplyBlackWatchMitigationRequest(request, userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                .startsWith("Unsupported resource type specified for BWiR"));
    }

    @Test
    public void testValidApplyBlackWatchMitigationGlobalResourceType() {
        ApplyBlackWatchMitigationRequest request = new ApplyBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());
        request.setResourceId("ResourceId123");
        request.setResourceType("GLB");

        String json = "{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"GLOBAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": 1000,"
                + "        \"global_bps\": 9999"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        request.setMitigationSettingsJSON(json);
        String userARN = "arn:aws:iam::005436146250:user/blackwatch";
        validator.validateApplyBlackWatchMitigationRequest(request, userARN);
    }

    @Test
    public void testValidUpdateBlackWatchMitigationBWiRIPAddress() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "IPAddress";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

    @Test
    public void testValidUpdateBlackWatchMitigationBWiRIPAddressList() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "IPAddressList";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

    @Test
    public void testValidUpdateBlackWatchMitigationBWiRElasticIP() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "ElasticIP";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

    @Test
    public void testInValidUpdateBlackWatchMitigationBWiRELB() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "ELB";

        Throwable caughtExcepion = null;
        try {
            final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                    request, resouceType, new BlackWatchTargetConfig(), userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                    .startsWith("Unsupported resource type specified for BWiR"));
    }

    @Test
    public void testInValidUpdateBlackWatchMitigationBWiRGLB() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"REGIONAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "GLB";

        Throwable caughtExcepion = null;
        try {
            final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                    request, resouceType, new BlackWatchTargetConfig(), userARN);
        } catch (Exception ex) {
            caughtExcepion = ex;
        }
        assertNotNull(caughtExcepion);
        assertTrue(caughtExcepion instanceof IllegalArgumentException);
        assertTrue(caughtExcepion.getMessage()
                .startsWith("Unsupported resource type specified for BWiR"));
    }

    @Test
    public void testValidUpdateBlackWatchMitigationGlobalGLB() {
        UpdateBlackWatchMitigationRequest request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationActionMetadata(MitigationActionMetadata.builder()
                .withUser("Username")
                .withToolName("Toolname")
                .withDescription("Description")
                .build());

        final String validMitigationId = "US-WEST-1_2016-02-05T00:43:04.6767Z_55";
        request.setMitigationId(validMitigationId);

        final String userARN = "arn:aws:iam::005436146250:user/blackwatch";

        final Long theRateLimit = 120L;

        String json = String.format("{"
                + "  \"global_deployment\": {"
                + "    \"placement_tags\": [\"GLOBAL\"]"
                + "  },"
                + "  \"mitigation_config\": {"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}", theRateLimit);

        request.setMitigationSettingsJSON(json);

        String resouceType = "GLB";
        final BlackWatchTargetConfig newTargetConfig = validator.validateUpdateBlackWatchMitigationRequest(
                request, resouceType, new BlackWatchTargetConfig(), userARN);
        assertSame(theRateLimit.longValue(), getDefaultRateLimit(newTargetConfig).longValue());
    }

}
