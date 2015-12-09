package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.AssertUtils;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.google.common.collect.ImmutableSet;

public class DDBBasedDeleteRequestStorageHandlerTest {
    private final String domain = "unit-test";
    private static AmazonDynamoDBClient localDynamoDBClient;
    private static RequestTableTestHelper testTableHelper;
    
    private static final String MITIGATION_1_NAME = "Mitigation1";
    private static final String MITIGATION_2_NAME = "Mitigation2";
    private static final ImmutableSet<String> defaultLocations = ImmutableSet.of("POP1");
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging(Level.WARN);
        localDynamoDBClient = DynamoDBTestUtil.get().getClient();
    }
    
    private AmazonDynamoDBClient spyDynamoDBClient;
    private DDBBasedDeleteRequestStorageHandler storageHandler;
    
    private CreateMitigationRequest existingRequest1;
    
    private CreateMitigationRequest existingRequest2;
    private DDBBasedCreateRequestStorageHandler createStorageHandler;
    
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
        
        spyDynamoDBClient = spy(localDynamoDBClient);
        storageHandler = spy(new DDBBasedDeleteRequestStorageHandler(spyDynamoDBClient, domain));

        // Create a new TemplateBasedRequestValidator for the create so calls to it won't match any verifications
        // we might want to do on the verifier for storageHandler
        TemplateBasedRequestValidator createOnlytemplateBasedValidator = mock(TemplateBasedRequestValidator.class);
        createStorageHandler = new DDBBasedCreateRequestStorageHandler(
                localDynamoDBClient, domain, createOnlytemplateBasedValidator);
        
        existingRequest1 = RequestTestHelper.generateCreateMitigationRequest(MITIGATION_1_NAME);
        createStorageHandler.storeRequestForWorkflow(existingRequest1, defaultLocations, tsdMetrics);
        
        existingRequest2 = RequestTestHelper.generateCreateMitigationRequest(MITIGATION_2_NAME);
        createStorageHandler.storeRequestForWorkflow(existingRequest2, defaultLocations, tsdMetrics);
    }
    
    private static MitigationRequestDescriptionWithLocations validateRequestInDDB(
            DeleteMitigationFromAllLocationsRequest request, Set<String> locations, long workflowId) 
    {
        return testTableHelper.validateRequestInDDB(
                request, new MitigationDefinition(), request.getMitigationVersion() + 1, locations, workflowId);
    }
    
    @Test
    public void testDeleteMitigation() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics);

        validateRequestInDDB(request, defaultLocations, workflowId);
    }
    
    @Test
    public void testDeleteMitigationWithNewTemplate() {
        assertThat(existingRequest1.getMitigationTemplate(), equalTo(MitigationTemplate.Router_RateLimit_Route53Customer));
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(
                MitigationTemplate.Router_CountMode_Route53Customer, MITIGATION_1_NAME, 1);
        
        IllegalArgumentException exception = AssertUtils.assertThrows(
                IllegalArgumentException.class, 
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), allOf(
                containsString(MitigationTemplate.Router_RateLimit_Route53Customer),
                containsString(MitigationTemplate.Router_CountMode_Route53Customer)));
    }
    
    @Test
    public void testDeleteNonExistentMitigation() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest("DoesNotExist", 1);
        
        MissingMitigationException400 exception = AssertUtils.assertThrows(MissingMitigationException400.class,
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), containsString(request.getMitigationName()));
    }
    
    @Test
    public void testDeleteNonExistentMitigationWithSameNameOnDifferentDevice() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(
                MitigationTemplate.Blackhole_Mitigation_ArborCustomer,
                existingRequest1.getMitigationName(),
                existingRequest1.getServiceName(),
                2);
        
        MissingMitigationException400 exception = AssertUtils.assertThrows(MissingMitigationException400.class,
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), containsString(request.getMitigationName()));
    }
    
    @Test
    public void testDuplicateDelete() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);

        validateRequestInDDB(request1, defaultLocations, workflowId);
        
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        StaleRequestException400 exception = AssertUtils.assertThrows(StaleRequestException400.class,
                () -> storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), allOf(
                containsString(request2.getMitigationName()), 
                containsString("version to delete: " + request2.getMitigationVersion())));
    }
    
    @Test
    public void testRetryFailedDelete() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request1, defaultLocations, workflowId);
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        testTableHelper.setWorkflowStatus(deviceNameAndScope.getDeviceName().name(), workflowId, WorkflowStatus.FAILED);
        
        // Retrying the same request should be allowed
        long newWorkflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request1, defaultLocations, newWorkflowId);
    }
    
    @Test
    public void testRetryPartialSuccess() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request1, defaultLocations, workflowId);
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        testTableHelper.setWorkflowStatus(deviceNameAndScope.getDeviceName().name(), workflowId, WorkflowStatus.PARTIAL_SUCCESS);
        
        // Need a new request with an updated version
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 2);
        long newWorkflowId = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request2, defaultLocations, newWorkflowId);
    }
    
    @Test
    public void testRetryIndeterminate() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request1, defaultLocations, workflowId);
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        testTableHelper.setWorkflowStatus(deviceNameAndScope.getDeviceName().name(), workflowId, WorkflowStatus.INDETERMINATE);
        
        // Need a new request with an updated version
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 2);
        long newWorkflowId = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request2, defaultLocations, newWorkflowId);
    }
    
    @Test
    public void testRetrySuccess() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        
        validateRequestInDDB(request1, defaultLocations, workflowId);
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        testTableHelper.setWorkflowStatus(deviceNameAndScope.getDeviceName().name(), workflowId, WorkflowStatus.SUCCEEDED);
        
        // New request with an updated version
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 2);
        
        DuplicateRequestException400 exception = AssertUtils.assertThrows(DuplicateRequestException400.class,
                () -> storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), allOf(
                containsString(request2.getMitigationName()), 
                containsString("version: " + request2.getMitigationVersion())));
    }
    
    @Test
    public void testHigherMitigationVersion() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 10);
        
        StaleRequestException400 exception = AssertUtils.assertThrows(StaleRequestException400.class,
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));
        
        assertThat(exception.getMessage(), containsString(request.getMitigationName()));
    }
    
    @Test
    public void testConcurrentDelete() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_2_NAME, 1);
        
        long workflowId2[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
            return i.callRealMethod();
        }));
        
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        validateRequestInDDB(request1, defaultLocations, workflowId1);
        validateRequestInDDB(request2, defaultLocations, workflowId2[0]);
        
        assertThat(workflowId1, greaterThan(workflowId2[0]));
        
        // Should be called 3 times - 1 failed with condition not met, plus the two actual puts
        verify(storageHandler, times(3)).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
    
    @Test
    public void testConcurrentDeleteOfSameMitigation() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId2[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
            return i.callRealMethod();
        }));
        
        AssertUtils.assertThrows(
                StaleRequestException400.class,
                () -> storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics));
        validateRequestInDDB(request2, defaultLocations, workflowId2[0]);
        
        // Should be called 2 times - one failed, one for the success and no retry for the failed as its now stale
        verify(storageHandler, times(2)).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
    
    @Test
    public void testConcurrentMixedDeletes() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_2_NAME, 1);
        DeleteMitigationFromAllLocationsRequest request3 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        long workflowId2[] = new long[1];
        long workflowId3[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Do the store for request 2 first
            workflowId2[0] = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
            return i.callRealMethod();
        }).doCallRealMethod() // Allow the call for request 2 to complete normally
        .doAnswer( i-> { 
            // Intercept the retry of request 1 and do request 3 instead
            workflowId3[0] = storageHandler.storeRequestForWorkflow(request3, defaultLocations, tsdMetrics);
            return i.callRealMethod();
        }).doCallRealMethod().doCallRealMethod()); // Calls for request 3 and final call for request 1
        
        AssertUtils.assertThrows(
                StaleRequestException400.class, 
                () -> storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics));
        
        validateRequestInDDB(request2, defaultLocations, workflowId2[0]);
        validateRequestInDDB(request3, defaultLocations, workflowId3[0]);
        
        // 2 retries + 2 puts
        verify(storageHandler, times(4)).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
    
    /*@Test
    public void testTooManyConcurrentDeletes() {
        DeleteMitigationFromAllLocationsRequest firstRequest = 
                RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 2);
        
        AtomicInteger mitigation2ExpectedVersion = new AtomicInteger(1);
        
        doAnswer(i -> {
            DeleteMitigationFromAllLocationsRequest request = 
                    RequestTestHelper.generateDeleteMitigationRequest(
                            MITIGATION_2_NAME, mitigation2ExpectedVersion.incrementAndGet());
            // Do the store for the other request first
            storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics);
            return i.callRealMethod(); 
        }).when(storageHandler).putItemInDDB(
                MockUtils.argThatMatchesPredicate(map ->  
                    map.containsKey(DDBRequestSerializer.MITIGATION_NAME_KEY) && 
                    MITIGATION_1_NAME.equals(map.get(DDBRequestSerializer.MITIGATION_NAME_KEY).getS())
                ),
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
        
        AssertUtils.assertThrows(
                ConditionalCheckFailedException.class, 
                () -> storageHandler.storeRequestForWorkflow(firstRequest, defaultLocations, tsdMetrics));
        
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_ACTIVITY_MAX_ATTEMPTS)).putItemInDDB(
            MockUtils.argThatMatchesPredicate(map ->  
                map.containsKey(DDBRequestSerializer.MITIGATION_NAME_KEY) && 
                MITIGATION_1_NAME.equals(map.get(DDBRequestSerializer.MITIGATION_NAME_KEY).getS())
            ),
            anyMapOf(String.class, ExpectedAttributeValue.class), 
            any(TSDMetrics.class));
    }*/
    
    @Test
    public void testConcurrentDeleteAndCreate() {
        DeleteMitigationFromAllLocationsRequest request1 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        CreateMitigationRequest request2 = RequestTestHelper.generateCreateMitigationRequest("NewMitigation");
        
        long workflowId2[] = new long[1];
        
        RequestTableTestHelper.whenAnyPut(storageHandler, doAnswer(i -> {
            // Restore the real call for the all the following calls
            RequestTableTestHelper.whenAnyPut(storageHandler, doCallRealMethod());
            // Do the store for request 2 first
            workflowId2[0] = createStorageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
            return i.callRealMethod();
        }));
        
        long workflowId1 = storageHandler.storeRequestForWorkflow(request1, defaultLocations, tsdMetrics);
        validateRequestInDDB(request1, defaultLocations, workflowId1);
        testTableHelper.validateRequestInDDB(
                request2, request2.getMitigationDefinition(), DDBBasedRequestStorageHandler.INITIAL_MITIGATION_VERSION,
                defaultLocations, workflowId2[0]);
        
        assertThat(workflowId1, greaterThan(workflowId2[0]));
        
        // Should be called 2 times - 1 failed with condition not met, plus the retry. The put for create doesn't count
        // as it doesn't go through the spy
        verify(storageHandler, times(2)).putItemInDDB(
                anyMapOf(String.class, AttributeValue.class), 
                anyMapOf(String.class, ExpectedAttributeValue.class), 
                any(TSDMetrics.class));
    }
    
    @Test
    public void testQueryFails() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        AmazonServiceException serviceException = new AmazonServiceException("Internal Error");
        serviceException.setErrorType(ErrorType.Service);
        doThrow(new AmazonServiceException("Internal Error")).when(spyDynamoDBClient).query(any(QueryRequest.class));
        
        // Don't sleep during the test
        doNothing().when(storageHandler).sleepForQueryRetry(anyInt());
        
        AssertUtils.assertThrows(
                AmazonServiceException.class,
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));

        verify(spyDynamoDBClient, times(DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS)).query(any(QueryRequest.class));
        
        ArgumentCaptor<Integer> attemptCountCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS - 1))
            .sleepForQueryRetry(attemptCountCaptor.capture());
        for (int i = 1; i < DDBBasedRequestStorageHandler.DDB_QUERY_MAX_ATTEMPTS; ++i) {
            assertEquals(i, attemptCountCaptor.getAllValues().get(i-1).intValue());
        }
    }
    

    @Test
    public void testPutFails() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        AmazonServiceException serviceException = new AmazonServiceException("Internal Error");
        serviceException.setErrorType(ErrorType.Service);
        doThrow(new AmazonServiceException("Internal Error")).when(spyDynamoDBClient).putItem(any(PutItemRequest.class));
        
        // Don't sleep during the test
        doNothing().when(storageHandler).sleepForPutRetry(anyInt());
        
        AssertUtils.assertThrows(
                AmazonServiceException.class,
                () -> storageHandler.storeRequestForWorkflow(request, defaultLocations, tsdMetrics));

        verify(spyDynamoDBClient, times(DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS)).putItem(any(PutItemRequest.class));
        
        ArgumentCaptor<Integer> attemptCountCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(storageHandler, times(DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS - 1))
            .sleepForPutRetry(attemptCountCaptor.capture());
        for (int i = 1; i < DDBBasedRequestStorageHandler.DDB_PUT_ITEM_MAX_ATTEMPTS; ++i) {
            assertEquals(i, attemptCountCaptor.getAllValues().get(i-1).intValue());
        }
    }
    
    @Test
    public void testHittingWorkflowIdLimit() {
        EditMitigationRequest request1 = RequestTestHelper.generateEditMitigationRequest(MITIGATION_1_NAME, 2);
        
        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request1.getMitigationTemplate());
        
        // bypass the normal API to store a request with 1 less than the max workflow id
        storageHandler.storeRequestInDDB(
                request1, request1.getMitigationDefinition(), defaultLocations, 
                deviceNameAndScope, deviceNameAndScope.getDeviceScope().getMaxWorkflowId() - 1, 
                RequestType.EditRequest, 2, tsdMetrics);
        
        DeleteMitigationFromAllLocationsRequest request2 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_2_NAME, 1);
        
        long workflowId = storageHandler.storeRequestForWorkflow(request2, defaultLocations, tsdMetrics);
        validateRequestInDDB(request2, defaultLocations, workflowId);
        assertEquals(deviceNameAndScope.getDeviceScope().getMaxWorkflowId(), workflowId);
        
        DeleteMitigationFromAllLocationsRequest request3 = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 2);
        
        AssertUtils.assertThrows(
                InternalServerError500.class, 
                () -> storageHandler.storeRequestForWorkflow(request3, defaultLocations, tsdMetrics));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void testUpdateRunIdForWorkflowRequest() {
        DeleteMitigationFromAllLocationsRequest request = RequestTestHelper.generateDeleteMitigationRequest(MITIGATION_1_NAME, 1);
        
        ImmutableSet<String> locations = ImmutableSet.of("POP1");
        long workflowId = storageHandler.storeRequestForWorkflow(request, locations, tsdMetrics);

        DeviceNameAndScope deviceNameAndScope = 
                MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        // There isn't a way to retrieve the id we stored
        storageHandler.updateRunIdForWorkflowRequest(
                deviceNameAndScope.getDeviceName().toString(), workflowId, "FakeRunId", tsdMetrics);
        validateRequestInDDB(request, locations, workflowId);
    }

}
