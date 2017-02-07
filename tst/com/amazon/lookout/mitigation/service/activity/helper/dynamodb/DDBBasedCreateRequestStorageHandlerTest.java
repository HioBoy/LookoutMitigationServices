package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Stubber;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.DuplicateMitigationNameException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.AssertUtils;
import com.amazon.lookout.test.common.util.MockUtils;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.ImmutableSet;

@RunWith(JUnitParamsRunner.class)
public class DDBBasedCreateRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private static final String domain = "unit-test";
    
    private static AmazonDynamoDBClient localDynamoDBClient;
    private static RequestTableTestHelper testTableHelper;
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging(Level.ERROR);
        localDynamoDBClient = DynamoDBTestUtil.get().getClient();
    }
    
    @Before
    public void setUpBeforeTest() throws InterruptedException {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        DynamoDB dynamoDB = new DynamoDB(localDynamoDBClient);
        
        try {
            Table table = dynamoDB.getTable(MitigationRequestsModel.getInstance().getTableName(domain));
            table.delete();
            table.waitForDelete();
        } catch (ResourceNotFoundException e) {
            // Already deleted
        }
        MitigationRequestsModel.getInstance().createTableIfNotExist(localDynamoDBClient, domain);
        testTableHelper = new RequestTableTestHelper(new DynamoDB(localDynamoDBClient), domain);
    }
    
    @Test
    public void testCreateMitigation() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();

        validateRequestInDDB(request, locations, workflowId);
    }
    
    private static MitigationRequestDescriptionWithLocations validateRequestInDDB(
            CreateMitigationRequest request, Set<String> locations, long workflowId) 
    {
        return testTableHelper.validateRequestInDDB(
                request, request.getMitigationDefinition(), DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION, 
                locations, workflowId);
    }
    private static void validateRequestNotInDDB(CreateMitigationRequest request) {
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        ItemCollection<QueryOutcome> items = testTableHelper.getRequestsWithName(deviceNameAndScope.getDeviceName().toString(), request.getMitigationName());
        
        Iterator<Item> itr = items.iterator();
        int count = 0;
        while (itr.hasNext()) {
            itr.next();
            count++;
        }
        
        if (count != 0) {
            fail("Expected no mitigation but found " + count + " mitigations.");
        }
    }
    
    @Test
    public void testDuplicateName() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();

        DuplicateMitigationNameException400 exception = AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics));
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        assertThat(exception.getMessage(), 
                allOf(containsString(request.getMitigationName()), containsString(deviceNameAndScope.getDeviceName().toString())));
        
        validateRequestInDDB(request, locations, workflowId);
    }
    
    @Test
    public void testDuplicateNameButDifferentDevice() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest(
                MitigationTemplate.Router_RateLimit_Route53Customer, "Name", ServiceName.Route53);
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request1, locations, workflowId1);
        
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest(
                MitigationTemplate.Blackhole_Mitigation_ArborCustomer, "Mitigation2", ServiceName.Blackhole);
        long workflowId2 = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request2, locations, workflowId2);
    }
    
    @Test
    public void testDuplicateNameButDifferentService() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest(
                MitigationTemplate.Router_RateLimit_Route53Customer, "Name", ServiceName.Route53);
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request1, locations, workflowId1);
        
        // There currently isn't a different service with the same template, or even device 
        // but we should handle the case if there is one
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest(
                MitigationTemplate.Router_RateLimit_Route53Customer, "Name", "FakeService");
        AssertUtils.assertThrows(DuplicateMitigationNameException400.class,
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
    
    /**
     * Test we can re-create a mitigation after it get successfully deleted, and the version is correctly bumped
     */
    @Test
    public void testRecreateSucceed() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
        
        validateRequestInDDB(request, locations, workflowId);
        
        deleteRequest(storageHandler, request, workflowId, locations);
        // set delete request to succeed.
        testTableHelper.setWorkflowStatus(DeviceName.POP_ROUTER.name(), workflowId + 1, WorkflowStatus.SUCCEEDED);
        
        // Recreate
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest();
        request2.getMitigationActionMetadata().setDescription("RecreatedRequest");
        long newWorkflowId = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
        assertEquals(newWorkflowId, 3);
        testTableHelper.validateRequestInDDB(request2, request2.getMitigationDefinition(), 3, locations, newWorkflowId); 
    }
     
    /**
     * Test we can not re-create if a mitigation is deleted in progress
     */
    @Test
    @Parameters({
        WorkflowStatus.RUNNING,
        WorkflowStatus.INDETERMINATE,
        WorkflowStatus.PARTIAL_SUCCESS,
    })
    public void testRecreateFailed1(String deleteRequestWorkflowStatus) {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
        
        validateRequestInDDB(request, locations, workflowId);
        
        deleteRequest(storageHandler, request, workflowId, locations);
        // set delete request to succeed.
        testTableHelper.setWorkflowStatus(DeviceName.POP_ROUTER.name(), workflowId + 1, deleteRequestWorkflowStatus);

        // Recreate
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest();
        request2.getMitigationActionMetadata().setDescription("RecreatedRequest");

        AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
     
    /**
     * Test we can not re-create if a mitigation is latest request is edit
     */
    @Test
    @Parameters({
        WorkflowStatus.RUNNING,
        WorkflowStatus.INDETERMINATE,
        WorkflowStatus.PARTIAL_SUCCESS,
        WorkflowStatus.SUCCEEDED,
    })
    public void testRecreateFailed2(String editRequestWorkflowStatus) {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
        
        validateRequestInDDB(request, locations, workflowId);
        
        editRequest(storageHandler, request, workflowId, locations);
        // set edit request status.
        testTableHelper.setWorkflowStatus(DeviceName.POP_ROUTER.name(), workflowId + 1, editRequestWorkflowStatus);

        // Recreate
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest();
        request2.getMitigationActionMetadata().setDescription("RecreatedRequest");

        AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
    
    /**
     * Test we can not re-create if a mitigation is latest request is create
     */
    @Test
    @Parameters({
        WorkflowStatus.RUNNING,
        WorkflowStatus.INDETERMINATE,
        WorkflowStatus.PARTIAL_SUCCESS,
        WorkflowStatus.SUCCEEDED,
    })
    public void testRecreateFailed3(String createRequestWorkflowStatus) {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
        
        validateRequestInDDB(request, locations, workflowId);
        
        // set edit request status.
        testTableHelper.setWorkflowStatus(DeviceName.POP_ROUTER.name(), workflowId, createRequestWorkflowStatus);

        // Recreate
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest();
        request2.getMitigationActionMetadata().setDescription("RecreatedRequest");

        AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
    
    /**
     * Test we can not re-create if a mitigation is latest request is rollback
     */
    @Test
    @Parameters({
        WorkflowStatus.RUNNING,
        WorkflowStatus.INDETERMINATE,
        WorkflowStatus.PARTIAL_SUCCESS,
        WorkflowStatus.SUCCEEDED,
    })
    public void testRecreateFailed4(String rollbackRequestWorkflowStatus) {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
        
        validateRequestInDDB(request, locations, workflowId);
        
        // set edit request status.
        rollbackRequest(storageHandler, request, workflowId, locations);
        testTableHelper.setWorkflowStatus(DeviceName.POP_ROUTER.name(), workflowId + 1, rollbackRequestWorkflowStatus);

        // Recreate
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest();
        request2.getMitigationActionMetadata().setDescription("RecreatedRequest");

        AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
    
    private void deleteRequest(
            DDBBasedRequestStorageHandler storeHandler, CreateMitigationRequest request, long requestWorkflowId,
            ImmutableSet<String> locations) 
    {
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        long maxWorkflowId = storeHandler.getMaxWorkflowIdForDevice(
                deviceNameAndScope.getDeviceName().toString(), deviceNameAndScope.getDeviceScope().toString(), null, tsdMetrics);
        
        // We shouldn't need to use another handler for this but the code is factored badly
        DeleteMitigationFromAllLocationsRequest deleteRequest = new DeleteMitigationFromAllLocationsRequest();
        deleteRequest.setMitigationName(request.getMitigationName());
        deleteRequest.setMitigationTemplate(request.getMitigationTemplate());
        deleteRequest.setServiceName(request.getServiceName());
        deleteRequest.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                    .withDescription("Test")
                    .withToolName("UnitTest")
                    .withUser("TestUser")
                    .build());
        deleteRequest.setMitigationVersion(DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION);
        long newWorkflowId = maxWorkflowId + 1;
        storeHandler.storeRequestInDDB(
                deleteRequest, null, locations, deviceNameAndScope, maxWorkflowId + 1, RequestType.DeleteRequest, 2, tsdMetrics);
        
        testTableHelper.setUpdateWorkflowId(deviceNameAndScope.getDeviceName().toString(), requestWorkflowId, newWorkflowId);
    }
     
    private void editRequest(
            DDBBasedRequestStorageHandler storeHandler, CreateMitigationRequest request, long requestWorkflowId,
            ImmutableSet<String> locations) 
    {
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        long maxWorkflowId = storeHandler.getMaxWorkflowIdForDevice(
                deviceNameAndScope.getDeviceName().toString(), deviceNameAndScope.getDeviceScope().toString(), null, tsdMetrics);
        
        // We shouldn't need to use another handler for this but the code is factored badly
        EditMitigationRequest editRequest = new EditMitigationRequest();
        editRequest.setMitigationName(request.getMitigationName());
        editRequest.setMitigationTemplate(request.getMitigationTemplate());
        editRequest.setServiceName(request.getServiceName());
        editRequest.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                    .withDescription("Test")
                    .withToolName("UnitTest")
                    .withUser("TestUser")
                    .build());
        editRequest.setMitigationVersion(DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION);
        long newWorkflowId = maxWorkflowId + 1;
        storeHandler.storeRequestInDDB(
                editRequest, null, locations, deviceNameAndScope, maxWorkflowId + 1, RequestType.EditRequest, 2, tsdMetrics);
        
        testTableHelper.setUpdateWorkflowId(deviceNameAndScope.getDeviceName().toString(), requestWorkflowId, newWorkflowId);
    }
    
    private void rollbackRequest(
            DDBBasedRequestStorageHandler storeHandler, CreateMitigationRequest request, long requestWorkflowId,
            ImmutableSet<String> locations) 
    {
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        long maxWorkflowId = storeHandler.getMaxWorkflowIdForDevice(
                deviceNameAndScope.getDeviceName().toString(), deviceNameAndScope.getDeviceScope().toString(), null, tsdMetrics);
        
        // We shouldn't need to use another handler for this but the code is factored badly
        RollbackMitigationRequest rollbackRequest = new RollbackMitigationRequest();
        rollbackRequest.setMitigationName(request.getMitigationName());
        rollbackRequest.setMitigationTemplate(request.getMitigationTemplate());
        rollbackRequest.setServiceName(request.getServiceName());
        rollbackRequest.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                    .withDescription("Test")
                    .withToolName("UnitTest")
                    .withUser("TestUser")
                    .build());
        rollbackRequest.setMitigationVersion(DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION);
        long newWorkflowId = maxWorkflowId + 1;
        storeHandler.storeRequestInDDB(
                rollbackRequest, null, locations, deviceNameAndScope, maxWorkflowId + 1, RequestType.RollbackRequest, 2, tsdMetrics);
        
        testTableHelper.setUpdateWorkflowId(deviceNameAndScope.getDeviceName().toString(), requestWorkflowId, newWorkflowId);
    }
    
    @Test
    public void testNonConflictingDefinitions() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest();
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request1, locations, workflowId1);
        
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("Mitigation2");
        long workflowId2 = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request2, locations, workflowId2);
    }
    
    private static void whenValidateCoexistence(
            TemplateBasedRequestValidator templateBasedValidator, 
            CreateMitigationRequest request1, CreateMitigationRequest request2, 
            Stubber stubber) 
    {
        stubber.when(templateBasedValidator).validateCoexistenceForTemplateAndDevice(
                eq(request2.getMitigationTemplate()), eq(request2.getMitigationName()), eq(request2.getMitigationDefinition()), 
                eq(request1.getMitigationTemplate()), eq(request1.getMitigationName()), eq(request1.getMitigationDefinition()),
                any(TSDMetrics.class));
    }
    
    @Test
    public void testConflictingDefinitions() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics).getWorkflowId();
        
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("ConflictingMitigation");
        
        whenValidateCoexistence(templateBasedValidator, request1, request2, 
                doThrow(new DuplicateDefinitionException400("Conflicting definitions")));
        
        AssertUtils.assertThrows(
                DuplicateDefinitionException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics));
    }
    
    @Test
    public void testConcurrentCreates() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = spy(new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest();
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("Mitigation2");
        
        long workflowId2[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod();
        }));
        
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request1, locations, workflowId1);
        validateRequestInDDB(request2, locations, workflowId2[0]);
        
        assertThat(workflowId1, greaterThan(workflowId2[0]));
        
        // Should be called 3 times - 1 failed with condition not met, plus the two actual puts
        verify(storageHandler, times(3)).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
    
    @Test
    public void testConcurrentDuplicateCreates() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = spy(new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        long workflowId2[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod();
        }));
        
        AssertUtils.assertThrows(
                DuplicateMitigationNameException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics));
        
        validateRequestInDDB(request, locations, workflowId2[0]);
    }
    
    @Test
    public void testConcurrentConflictingCreates() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = spy(new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest();
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("ConflictingMitigation");
        
        whenValidateCoexistence(templateBasedValidator, request2, request1, 
                doThrow(new DuplicateDefinitionException400("Conflicting definitions")));

        long workflowId2[] = new long[1];
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod();
        }));
        
        AssertUtils.assertThrows(
                DuplicateDefinitionException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics));
        
        validateRequestNotInDDB(request1);
        validateRequestInDDB(request2, locations, workflowId2[0]);
    }
    
    @Test
    public void testConcurrentMixedCreates() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = spy(new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest();
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("NonConflictingMitigation");
        CreateMitigationRequest request3 = RequestTestHelper.generateCreateMitigationRequest("ConflictingMitigation");
        
        whenValidateCoexistence(templateBasedValidator, request3, request1, 
                doThrow(new DuplicateDefinitionException400("Conflicting definitions")));
        
        long workflowId2[] = new long[1];
        long workflowId3[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod();
        }).doCallRealMethod() // Allow the call for request 2 to complete normally
        .doAnswer( i-> { 
            // Intercept the retry of request 1 and do request 3 instead
            workflowId3[0] = storageHandler.storeRequestForWorkflow(request3, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod();
        }).doCallRealMethod().doCallRealMethod()); // Calls for request 3 and final call for request 1
        
        AssertUtils.assertThrows(
                DuplicateDefinitionException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request1, locations, tsdMetrics));
        
        validateRequestNotInDDB(request1);
        validateRequestInDDB(request2, locations, workflowId2[0]);
        validateRequestInDDB(request3, locations, workflowId3[0]);
    }
    
    @Test
    public void testTooManyConcurrentCreates() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        DDBBasedCreateRequestStorageHandler storageHandler = spy(new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, templateBasedValidator));
        
        String initialRequestName = "InitialRequest";
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest(initialRequestName);
        
        AtomicInteger requestCount = new AtomicInteger(0);
        
        doAnswer(i -> {
            CreateMitigationRequest newRequest = RequestTestHelper.generateCreateMitigationRequest("Request-" + requestCount.incrementAndGet());
            // Do the store for the other request first
            storageHandler.storeRequestForWorkflow(newRequest, locations, tsdMetrics).getWorkflowId();
            return i.callRealMethod(); 
        }).when(storageHandler).putItemInDDB(
                MockUtils.argThatMatchesPredicate(map ->  
                    map.containsKey(MitigationRequestsModel.MITIGATION_NAME_KEY) && 
                    initialRequestName.equals(map.get(MitigationRequestsModel.MITIGATION_NAME_KEY).getS())
                ),
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
        
        AssertUtils.assertThrows(
                ConditionalCheckFailedException.class, 
                () -> storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics));
        
        validateRequestNotInDDB(request);
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_ACTIVITY_MAX_ATTEMPTS)).putItemInDDB(
                MockUtils.argThatMatchesPredicate(map ->  
                map.containsKey(MitigationRequestsModel.MITIGATION_NAME_KEY) && 
                initialRequestName.equals(map.get(MitigationRequestsModel.MITIGATION_NAME_KEY).getS())
            ),
            anyMapOf(String.class, ExpectedAttributeValue.class), 
            any(TSDMetrics.class));
    }
    
    @Test
    public void testQueryFails() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedCreateRequestStorageHandler storageHandler =
                spy(new DDBBasedCreateRequestStorageHandler(spyClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        AmazonServiceException serviceException = new AmazonServiceException("Internal Error");
        serviceException.setErrorType(ErrorType.Service);
        doThrow(new AmazonServiceException("Internal Error")).when(spyClient).query(any(QueryRequest.class));
        
        // Don't sleep during the test
        doNothing().when(storageHandler).sleepForQueryRetry(anyInt());
        
        AssertUtils.assertThrows(
                AmazonServiceException.class,
                () -> storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics));

        // Reset so we can query
        doCallRealMethod().when(spyClient).query(any(QueryRequest.class));
        
        validateRequestNotInDDB(request);
        
        verify(spyClient, times(DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS)).query(any(QueryRequest.class));
        
        ArgumentCaptor<Integer> attemptCountCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS - 1))
            .sleepForQueryRetry(attemptCountCaptor.capture());
        for (int i = 1; i < DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS; ++i) {
            assertEquals(i, attemptCountCaptor.getAllValues().get(i-1).intValue());
        }
    }
    
    @Test
    public void testPutFails() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedCreateRequestStorageHandler storageHandler =
                spy(new DDBBasedCreateRequestStorageHandler(spyClient, domain, templateBasedValidator));
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        AmazonServiceException serviceException = new AmazonServiceException("Internal Error");
        serviceException.setErrorType(ErrorType.Service);
        doThrow(new AmazonServiceException("Internal Error")).when(spyClient).putItem(any(PutItemRequest.class));
        
        // Don't sleep during the test
        doNothing().when(storageHandler).sleepForPutRetry(anyInt());
        
        AssertUtils.assertThrows(
                AmazonServiceException.class,
                () -> storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics));

        validateRequestNotInDDB(request);
        
        verify(spyClient, times(DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS)).putItem(any(PutItemRequest.class));
        
        ArgumentCaptor<Integer> attemptCountCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS - 1))
            .sleepForPutRetry(attemptCountCaptor.capture());
        for (int i = 1; i < DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS; ++i) {
            assertEquals(i, attemptCountCaptor.getAllValues().get(i-1).intValue());
        }
    }
    
    // This test should try and force us to use pagination
    @Test
    public void testLotsOfMitigations() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(
                spyClient, domain, templateBasedValidator);
        
        // Limit the number of items in a query - otherwise creating enough items to need
        // pagination takes far to long
        doAnswer(i -> {
            QueryRequest request = (QueryRequest) i.getArguments()[0];
            QueryRequest clonedRequest = request.clone();
            if (request.getLimit() != null) {
                clonedRequest.setLimit(Math.max(Math.min(request.getLimit() / 2, 5), 1));
            } else {
                clonedRequest.setLimit(5);
            }
            // bypass the spy to make the real call
            return localDynamoDBClient.query(clonedRequest);
        }).when(spyClient).query(any(QueryRequest.class));
        
        for (int i = 0; i < 25; i++) {
            CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest("Mitigation" + i);
            long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();
            validateRequestInDDB(request, locations, workflowId);
        }
    }
    
    @Test
    public void testHittingWorkflowIdLimit() {
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedCreateRequestStorageHandler storageHandler = 
                new DDBBasedCreateRequestStorageHandler(spyClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request1 = RequestTestHelper.generateCreateMitigationRequest("Mitigation1");
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        // bypass the normal API to store a request with 1 less than the max workflow id
        storageHandler.storeRequestInDDB(
                request1, request1.getMitigationDefinition(), locations, 
                deviceNameAndScope, deviceNameAndScope.getDeviceScope().getMaxWorkflowId() - 1, 
                RequestType.CreateRequest, DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION,
                tsdMetrics);
        
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("Mitigation2");
        long workflowId = storageHandler.storeRequestForWorkflow(request2, locations, tsdMetrics).getWorkflowId();
        validateRequestInDDB(request2, locations, workflowId);
        assertEquals(deviceNameAndScope.getDeviceScope().getMaxWorkflowId(), workflowId);
        
        CreateMitigationRequest request3 = RequestTestHelper.generateCreateMitigationRequest("Mitigation3");
        
        AssertUtils.assertThrows(
                InternalServerError500.class, 
                () -> storageHandler.storeRequestForWorkflow(request3, locations, tsdMetrics));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testUpdateRunIdForWorkflowRequest() {
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        when(templateBasedValidator.requiresCheckForDuplicateAndConflictingRequests(anyString())).thenReturn(true);
        
        AmazonDynamoDBClient spyClient = spy(localDynamoDBClient);
        
        DDBBasedCreateRequestStorageHandler storageHandler =
                new DDBBasedCreateRequestStorageHandler(spyClient, domain, templateBasedValidator);
        
        
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest();
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics).getWorkflowId();

        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        // There isn't a way to retrieve the id we stored
        storageHandler.updateRunIdForWorkflowRequest(
                deviceNameAndScope.getDeviceName().toString(), workflowId, "FakeRunId", tsdMetrics);
        validateRequestInDDB(request, locations, workflowId);
    }
}
