package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.request.RollbackMitigationRequestInternal;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Lists;

import static com.amazon.lookout.mitigation.service.activity.helper.dynamodb.RequestTableTestHelper.*;

@SuppressWarnings("unchecked")
public class DDBBasedRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private static final String domain = "beta";
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DynamoDB dynamodb;
    private static DDBBasedRequestStorageHandler mitigationInfoHandler;
    private static String tableName;
    private static RequestTableTestHelper requestTableTestHelper;
 
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        dynamodb = new DynamoDB(dynamoDBClient);
        mitigationInfoHandler = new DDBBasedRequestStorageHandler(dynamoDBClient, domain);
        tableName = MitigationRequestsModel.getInstance().getTableName(domain);
        requestTableTestHelper = new RequestTableTestHelper(dynamodb, domain);
    }

    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        CreateTableRequest request = MitigationRequestsModel.getInstance().getCreateTableRequest(tableName);
        if (Tables.doesTableExist(dynamoDBClient, tableName)) {
            dynamoDBClient.deleteTable(tableName);
        }
        dynamoDBClient.createTable(request);
        mitigationInfoHandler = new DDBBasedRequestStorageHandler(dynamoDBClient, domain);
    }
    
    /**
     * Test the happy case where the StorageHandler is able to store everything just fine into DDB.
     */
    @Test
    public void testHappyCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());

        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }

        assertNull(caughtException);
        verify(storageHandler, times(1)).putItemInDDB(anyMap(), anyMap(), any(TSDMetrics.class));
    }

    /**
     * Test the case where the StorageHandler throws transient exceptions, but succeeds on retrying.
     */
    @Test
    public void testTransientFailuresCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(storageHandler).putItemInDDB(anyMap(), anyMap(), any(TSDMetrics.class));
        when(storageHandler.getSleepMillisMultiplierOnPutRetry()).thenReturn(1);

        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }

        assertNull(caughtException);
        verify(storageHandler, times(3)).putItemInDDB(anyMap(), anyMap(), any(TSDMetrics.class));
    }

    /**
     * Test the case where the StorageHandler throws transient exceptions when storing and never succeeds in any of the DDB_PUT_ITEM_MAX_ATTEMPTS number of attempts.
     * We expect an exception to the be thrown in this case.
     */
    @Test
    public void testStorageFailuresCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        doThrow(new RuntimeException()).when(storageHandler).putItemInDDB(anyMap(), anyMap(), any(TSDMetrics.class));
        when(storageHandler.getSleepMillisMultiplierOnPutRetry()).thenReturn(1);

        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, mitigationDefinition, Sets.newHashSet("TST1"), deviceNameAndScope, workflowId, requestType, mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }

        assertNotNull(caughtException);
        verify(storageHandler, times(3)).putItemInDDB(anyMap(), anyMap(), any(TSDMetrics.class));
    }

    /**
     * Test the attributes to store generated by the DDBBasedRequestStorageHandler.
     */
    @Test
    public void testGenerateAttributesToStore() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        CreateMitigationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        request.setLocations(Lists.newArrayList("POP1", "POP2"));
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        Long workflowId = (long) 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;

        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        when(storageHandler.getJSONDataConverter()).thenReturn(jsonDataConverter);
        when(storageHandler.generateAttributesToStore(any(MitigationModificationRequest.class), any(MitigationDefinition.class), anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt())).thenCallRealMethod();

        Map<String, AttributeValue> attributesToStore = storageHandler.generateAttributesToStore(request,
                request.getMitigationDefinition(), Sets.newHashSet(request.getLocations()),
                deviceNameAndScope, workflowId, requestType, mitigationVersion);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY));
        String deviceNameString = attributesToStore.get(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY).getS();
        assertEquals(deviceNameString, deviceNameAndScope.getDeviceName().name());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY));
        Long newWorkflowId = Long.parseLong(attributesToStore.get(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY).getN());
        assertEquals(newWorkflowId, workflowId);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY));
        String deviceScope = attributesToStore.get(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY).getS();
        assertEquals(deviceScope, deviceNameAndScope.getDeviceScope().name());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY));
        String workflowStatus = attributesToStore.get(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY).getS();
        assertEquals(workflowStatus, WorkflowStatus.RUNNING);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY));
        String mitigationName = attributesToStore.get(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY).getS();
        assertEquals(mitigationName, request.getMitigationName());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY));
        String mitigationTemplate = attributesToStore.get(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY).getS();
        assertEquals(mitigationTemplate, request.getMitigationTemplate());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY));
        int newMitigationVersion = Integer.parseInt(attributesToStore.get(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY).getN());
        assertEquals(newMitigationVersion, mitigationVersion);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.SERVICE_NAME_KEY));
        String serviceName = attributesToStore.get(DDBBasedRequestStorageHandler.SERVICE_NAME_KEY).getS();
        assertEquals(serviceName, request.getServiceName());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.REQUEST_DATE_KEY));
        String requestDateString = attributesToStore.get(DDBBasedRequestStorageHandler.REQUEST_DATE_KEY).getN();
        DateTime requestDateTime = new DateTime(Long.parseLong(requestDateString));
        DateTime now = new DateTime(DateTimeZone.UTC);
        Duration duration = new Duration(requestDateTime, now);
        // An heuristic in the test here to check, since we set the time right before we store, to approximate the
        // check for specific time stored, we simply check if the time stored was less than or equals to a second.
        assertTrue(duration.getStandardSeconds() <= 1);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY));
        String requestTypeString = attributesToStore.get(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY).getS();
        assertEquals(requestTypeString, requestType.name());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.USERNAME_KEY));
        String userNameString = attributesToStore.get(DDBBasedRequestStorageHandler.USERNAME_KEY).getS();
        assertEquals(userNameString, request.getMitigationActionMetadata().getUser());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.TOOL_NAME_KEY));
        String toolNameString = attributesToStore.get(DDBBasedRequestStorageHandler.TOOL_NAME_KEY).getS();
        assertEquals(toolNameString, request.getMitigationActionMetadata().getToolName());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.USER_DESC_KEY));
        String descriptionString = attributesToStore.get(DDBBasedRequestStorageHandler.USER_DESC_KEY).getS();
        assertEquals(descriptionString, request.getMitigationActionMetadata().getDescription());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.RELATED_TICKETS_KEY));
        List<String> relatedTkts = attributesToStore.get(DDBBasedRequestStorageHandler.RELATED_TICKETS_KEY).getSS();
        assertEquals(relatedTkts, request.getMitigationActionMetadata().getRelatedTickets());

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.LOCATIONS_KEY));
        List<String> locations = attributesToStore.get(DDBBasedRequestStorageHandler.LOCATIONS_KEY).getSS();
        assertEquals(Sets.newHashSet(locations), Sets.newHashSet(request.getLocations()));

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY));
        String newMitigationDefinitionAsJsonStr = attributesToStore.get(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY).getS();
        String mitigationDefinitionAsJson = jsonDataConverter.toData(request.getMitigationDefinition());
        assertEquals(newMitigationDefinitionAsJsonStr, mitigationDefinitionAsJson);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_HASH_KEY));
        int newMitigationHash = Integer.parseInt(attributesToStore.get(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_HASH_KEY).getN());
        int mitigationHash = mitigationDefinitionAsJson.hashCode();
        assertEquals(newMitigationHash, mitigationHash);

        assertTrue(attributesToStore.containsKey(DDBBasedRequestStorageHandler.PRE_DEPLOY_CHECKS_DEFINITION_KEY));
        String newPreDeployChecks = attributesToStore.get(DDBBasedRequestStorageHandler.PRE_DEPLOY_CHECKS_DEFINITION_KEY).getS();
        String preDeployChecks = jsonDataConverter.toData(request.getPreDeploymentChecks());
        assertEquals(newPreDeployChecks, preDeployChecks);

        assertFalse(attributesToStore.containsKey(DDBBasedRequestStorageHandler.POST_DEPLOY_CHECKS_DEFINITION_KEY));
    }

    /**
     * Test the attributes to store generated by the DDBBasedRequestStorageHandler.
     */
    @Test
    public void testGenerateExpectationConditions() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        CreateMitigationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        request.setLocations(Lists.newArrayList("POP1", "POP2"));
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        Long workflowId = (long) 1;

        when(storageHandler.generateExpectedConditions(any(DeviceNameAndScope.class), anyLong())).thenCallRealMethod();
        Map<String, ExpectedAttributeValue> expectations = storageHandler.generateExpectedConditions(deviceNameAndScope, workflowId);

        assertEquals(expectations.size(), 2);
        assertTrue(expectations.containsKey(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY));
        assertTrue(expectations.containsKey(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY));

        ExpectedAttributeValue notExistsCondition = new ExpectedAttributeValue(false);
        assertEquals(expectations.get(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY), notExistsCondition);
        assertEquals(expectations.get(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY), notExistsCondition);
    }

    /**
     * Test the case where the update for swf runId goes through just fine.
     */
    @Test
    public void testUpdateSWFRunId() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        doCallRealMethod().when(storageHandler).updateRunIdForWorkflowRequest(anyString(), anyLong(), anyString(), any(TSDMetrics.class));

        Throwable caughtException = null;
        try {
            storageHandler.updateRunIdForWorkflowRequest("TestDevice", (long) 1, "TestSWFRunId", TestUtils.newNopTsdMetrics());
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);

        verify(storageHandler, times(1)).updateItemInDynamoDB(anyMap(), anyMap(), anyMap());
    }

    /**
     * Test the case where the update for swf runId has a couple of exceptions, but goes through just fine on retrying.
     */
    @Test
    public void testUpdateSWFRunIdAfterRetries() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(storageHandler).updateItemInDynamoDB(anyMap(), anyMap(), anyMap());
        when(storageHandler.getSleepMillisMultiplierOnUpdateRetry()).thenReturn(1);

        doCallRealMethod().when(storageHandler).updateRunIdForWorkflowRequest(anyString(), anyLong(), anyString(), any(TSDMetrics.class));

        Throwable caughtException = null;
        try {
            storageHandler.updateRunIdForWorkflowRequest("TestDevice", (long) 1, "TestSWFRunId", TestUtils.newNopTsdMetrics());
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);

        verify(storageHandler, times(3)).updateItemInDynamoDB(anyMap(), anyMap(), anyMap());
    }

    /**
     * Test the case where the update for swf runId doesn't goes through even after retries.
     */
    @Test
    public void testUnSuccessfulUpdateSWFRunId() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        doThrow(new RuntimeException()).when(storageHandler).updateItemInDynamoDB(anyMap(), anyMap(), anyMap());
        when(storageHandler.getSleepMillisMultiplierOnUpdateRetry()).thenReturn(1);

        doCallRealMethod().when(storageHandler).updateRunIdForWorkflowRequest(anyString(), anyLong(), anyString(), any(TSDMetrics.class));

        Throwable caughtException = null;
        try {
            storageHandler.updateRunIdForWorkflowRequest("TestDevice", (long) 1, "TestSWFRunId", TestUtils.newNopTsdMetrics());
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        verify(storageHandler, times(5)).updateItemInDynamoDB(anyMap(), anyMap(), anyMap());
    }

    /**
     * Test the keys that are generated for querying active mitigations for a device.
     */
    @Test
    public void testGetKeysForActiveMitigationsForDevice() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, "test", templateBasedValidator);

        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateRateLimitMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        Map<String, Condition> keyValues = storageHandler.getKeysForActiveMitigationsForDevice(deviceNameAndScope.getDeviceName().name());

        assertTrue(keyValues.containsKey(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY));
        Condition condition = keyValues.get(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY);
        assertEquals(condition.getComparisonOperator(), ComparisonOperator.EQ.name());
        assertEquals(condition.getAttributeValueList().size(), 1);
        assertEquals(condition.getAttributeValueList().get(0), new AttributeValue(deviceNameAndScope.getDeviceName().name()));

        assertTrue(keyValues.containsKey(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY));
        condition = keyValues.get(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY);
        assertEquals(condition.getComparisonOperator(), ComparisonOperator.EQ.name());
        assertEquals(condition.getAttributeValueList().size(), 1);
        assertEquals(condition.getAttributeValueList().get(0), new AttributeValue().withN("0"));
    }

    /**
     * Test the keys that are generated for querying mitigations for a device also constraining by a workflowId.
     */
    @Test
    public void testGetKeysForDeviceAndWorkflowId() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, "test", templateBasedValidator);

        long workflowId = 5;
        MitigationModificationRequest request = DDBBasedDeleteRequestStorageHandlerTest.createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        Map<String, Condition> keyValues = storageHandler.getKeysForDeviceAndWorkflowId(deviceNameAndScope.getDeviceName().name(), workflowId);

        assertTrue(keyValues.containsKey(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY));
        Condition condition = keyValues.get(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY);
        assertEquals(condition.getComparisonOperator(), ComparisonOperator.EQ.name());
        assertEquals(condition.getAttributeValueList().size(), 1);
        assertEquals(condition.getAttributeValueList().get(0), new AttributeValue(deviceNameAndScope.getDeviceName().name()));

        assertTrue(keyValues.containsKey(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY));
        condition = keyValues.get(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY);
        assertEquals(condition.getComparisonOperator(), ComparisonOperator.GE.name());
        assertEquals(condition.getAttributeValueList().size(), 1);
        assertEquals(condition.getAttributeValueList().get(0), new AttributeValue().withN(String.valueOf(workflowId)));
    }

    /**
     * Test the happy case for getting the max workflowId from the existing workflows in DDB.
     */
    @Test
    public void testGettingMaxWorkflowId() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));

        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);

        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, null, tsdMetrics);

        assertEquals((long) workflowId, workflowIdToReturn);
        verify(storageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
    }

    /**
     * Test the happy case for getting the max workflowId from the existing workflows in DDB, when we have a non-null lastEvaluatedKey from the previous query.
     */
    @Test
    public void testGettingMaxWorkflowIdWithLastEvaluatedKey() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, Condition> keyConditions = new HashMap<>();
        Set<AttributeValue> keyValues = Collections.singleton(new AttributeValue(deviceName));
        Condition condition = new Condition();
        condition.setComparisonOperator(ComparisonOperator.EQ);
        condition.setAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.DEVICE_NAME_KEY, condition);
        keyValues = Collections.singleton(new AttributeValue().withN(String.valueOf(workflowIdToReturn)));
        condition = new Condition().withComparisonOperator(ComparisonOperator.GE).withAttributeValueList(keyValues);
        keyConditions.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, condition);

        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();

        QueryResult result1 = new QueryResult().withCount(0).withLastEvaluatedKey(lastEvaluatedKey);

        QueryRequest request1 = new QueryRequest();
        request1.setAttributesToGet(Sets.newHashSet("WorkflowId"));
        request1.setTableName(null);
        request1.setConsistentRead(true);
        request1.setKeyConditions(keyConditions);
        request1.setScanIndexForward(false);
        request1.setLimit(5);

        AttributeValue deviceScopeAttrVal = new AttributeValue(deviceScope);
        condition = new Condition().withComparisonOperator(ComparisonOperator.EQ).withAttributeValueList(Arrays.asList(deviceScopeAttrVal));
        request1.addQueryFilterEntry(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, condition);

        TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedRequestStorageHelper.getMaxWorkflowIdForDevice");
        when(storageHandler.queryDynamoDB(request1, subMetrics)).thenReturn(result1);

        QueryRequest request2 = new QueryRequest();
        request2.setAttributesToGet(Sets.newHashSet("WorkflowId"));
        request2.setTableName(null);
        request2.setConsistentRead(true);
        request2.setKeyConditions(keyConditions);
        request2.setExclusiveStartKey(lastEvaluatedKey);
        request2.setScanIndexForward(false);
        request2.setLimit(5);
        request2.addQueryFilterEntry(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, condition);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));
        QueryResult result2 = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(request2, subMetrics)).thenReturn(result2);

        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        when(storageHandler.getKeysForDeviceAndWorkflowId(anyString(), anyLong())).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, workflowIdToReturn, tsdMetrics);

        assertEquals((long) workflowId, workflowIdToReturn);
        verify(storageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        verify(storageHandler, times(2)).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
    }

    /**
     * Test the happy case for getting the max workflowId from the existing workflows in DDB, succeeding after retrying after receiving some transient failures.
     */
    @Test
    public void testGettingMaxWorkflowIdTransientFailure() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));

        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);

        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, null, tsdMetrics);

        assertEquals((long) workflowId, workflowIdToReturn);
        verify(storageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
    }

    @Test
    public void testGettingLatestVersion() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        String mitigationName = "mitigation name";
        
        Integer latestMitigationVersion = 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion - 2)));
        
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion)));

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion - 1)));
        
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("DeviceName", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                          .withAttributeValueList(Collections.singleton(new AttributeValue(deviceName))));
        keyConditions.put("MitigationName", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                              .withAttributeValueList(Collections.singleton(new AttributeValue(mitigationName))));
        
        QueryRequest request = new QueryRequest().withAttributesToGet(Collections.singleton("MitigationVersion"))
                                                 .withTableName(null)
                                                 .withIndexName(DDBBasedRequestStorageHandler.MITIGATION_NAME_LSI)
                                                 .withConsistentRead(true)
                                                 .withKeyConditions(keyConditions);
        
        // Filter out any records whose DeviceScope isn't the same.
        request.addQueryFilterEntry("DeviceScope", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                                  .withAttributeValueList(Collections.singleton(new AttributeValue(deviceScope))));
        
        // Filter out any records whose status is Failed.
        request.addQueryFilterEntry("WorkflowStatus", new Condition().withComparisonOperator(ComparisonOperator.NE)
                                                                     .withAttributeValueList(Collections.singleton(new AttributeValue(WorkflowStatus.FAILED))));
        
        TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2, item3));
        when(storageHandler.queryDynamoDB(request, tsdMetrics)).thenReturn(result);

        when(storageHandler.getLatestVersionForMitigationOnDevice(anyString(), anyString(), anyString(), anyInt(), any(TSDMetrics.class))).thenCallRealMethod();
        Integer mitigationVersionToReturn = storageHandler.getLatestVersionForMitigationOnDevice(deviceName, deviceScope, mitigationName, null, tsdMetrics);

        assertEquals(latestMitigationVersion, mitigationVersionToReturn);
    }
    
    @Test
    public void testGetLatestVersionWithKnownPreviousVersion() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        String mitigationName = "mitigation name";
        Integer latestVersionOnLastAttempt = 1;
        
        Integer latestMitigationVersion = 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion - 2)));
        
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion)));

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(latestMitigationVersion - 1)));
        
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("DeviceName", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                       .withAttributeValueList(Collections.singleton(new AttributeValue(deviceName))));
        keyConditions.put("MitigationName", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                           .withAttributeValueList(Collections.singleton(new AttributeValue(mitigationName))));
        
        QueryRequest request = new QueryRequest().withAttributesToGet(Collections.singleton("MitigationVersion"))
                                                 .withTableName(null)
                                                 .withIndexName(DDBBasedRequestStorageHandler.MITIGATION_NAME_LSI)
                                                 .withConsistentRead(true)
                                                 .withKeyConditions(keyConditions);
        
        // Filter out any records whose DeviceScope isn't the same.
        request.addQueryFilterEntry("DeviceScope", new Condition().withComparisonOperator(ComparisonOperator.EQ)
                                                                  .withAttributeValueList(Collections.singleton(new AttributeValue(deviceScope))));
        
        // Filter out any records whose status is Failed.
        request.addQueryFilterEntry("WorkflowStatus", new Condition().withComparisonOperator(ComparisonOperator.NE)
                                                                     .withAttributeValueList(Collections.singleton(new AttributeValue(WorkflowStatus.FAILED))));
        
        request.addQueryFilterEntry("MitigationVersion", new Condition().withComparisonOperator(ComparisonOperator.GE)
                                                                        .withAttributeValueList(Collections.singleton(new AttributeValue().withN(String.valueOf(latestVersionOnLastAttempt)))));
        
        TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
        
        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2, item3));
        when(storageHandler.queryDynamoDB(request, tsdMetrics)).thenReturn(result);

        when(storageHandler.getLatestVersionForMitigationOnDevice(anyString(), anyString(), anyString(), anyInt(), any(TSDMetrics.class))).thenCallRealMethod();
        Integer mitigationVersionToReturn = storageHandler.getLatestVersionForMitigationOnDevice(deviceName, deviceScope, mitigationName, latestVersionOnLastAttempt, tsdMetrics);

        assertEquals(latestMitigationVersion, mitigationVersionToReturn);
    }

    /**
     * Test the case of querying for active mitigations on a device.
     */
    @Test
    public void testGetActiveMitigationsForDevice() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));

        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);

        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenCallRealMethod();
        QueryResult returnedResult = storageHandler.getActiveMitigationsForDevice(deviceName, deviceScope, Sets.newHashSet("WorkflowId"), new HashMap<String, Condition>(), new HashMap<String, AttributeValue>(), null, new HashMap<String, Condition>(), tsdMetrics);

        assertEquals(returnedResult, result);
        verify(storageHandler, times(1)).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
    }

    /**
     * Test the case of querying for active mitigations on a device with some transient failures.
     */
    @Test
    public void testGetActiveMitigationsForDeviceTransientFailures() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));

        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenReturn(result);

        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenCallRealMethod();
        QueryResult returnedResult = storageHandler.getActiveMitigationsForDevice(deviceName, deviceScope, Sets.newHashSet("WorkflowId"), new HashMap<String, Condition>(), new HashMap<String, AttributeValue>(), null, new HashMap<String, Condition>(), tsdMetrics);

        assertEquals(returnedResult, result);
        verify(storageHandler, times(3)).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
    }

    /**
     * Test creation of the queryFilters when querying for maxWorkflowId. 
     */
    @Test
    public void testQueryFiltersForMaxWorkflowId() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);

        String deviceScope = DeviceScope.GLOBAL.name();

        long workflowIdToReturn = (long) 3;

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn)));

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("WorkflowId", new AttributeValue().withN(String.valueOf(workflowIdToReturn - 1)));

        QueryResult result = new QueryResult().withCount(2).withItems(Lists.newArrayList(item1, item2));
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);

        when(storageHandler.createQueryFiltersForMaxWorkflowId(anyString())).thenCallRealMethod();

        Map<String, Condition> queryFilters = storageHandler.createQueryFiltersForMaxWorkflowId(deviceScope);

        assertEquals(queryFilters.size(), 1);
        assertTrue(queryFilters.containsKey("DeviceScope"));
        assertEquals(queryFilters.get("DeviceScope").getAttributeValueList().get(0).getS(), deviceScope);
        assertEquals(queryFilters.get("DeviceScope").getComparisonOperator(), ComparisonOperator.EQ.name());
    }
 
    /**
     * Test mitigation version outdated
     */
    @Test(expected = StaleRequestException400.class)
    public void testOutDatedMitigaitonVersion() {
        MitigationRequestItemCreator itemCreator = requestTableTestHelper
                .getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        int latestVersionInDDB = 100;
        itemCreator.setLocations(locations);
        itemCreator.setMitigationVersion(latestVersionInDDB);
        itemCreator.setWorkflowId(100);
        itemCreator.addItem();
        
        RollbackMitigationRequestInternal request = new RollbackMitigationRequestInternal();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        
        int requestVersion = 99;
        mitigationInfoHandler.storeUpdateMitigationRequest(request, mitigationDefinition,
                requestVersion, RequestType.RollbackRequest, locations, tsdMetrics);
    }
   
    /**
     * Another thread has just wrote the record after we get workflowId, and versionId
     */
    @Test
    public void testAnotherThreadWroteRecordAlready() {
        MitigationRequestItemCreator itemCreator = requestTableTestHelper
                .getItemCreator(deviceName, serviceName, mitigationName, deviceScope);
        int latestVersionInDDB = 100;
        itemCreator.setLocations(locations);
        itemCreator.setMitigationVersion(latestVersionInDDB);
        itemCreator.setWorkflowId(100);
        itemCreator.addItem();
        
        RollbackMitigationRequestInternal request = new RollbackMitigationRequestInternal();
        request.setMitigationName(mitigationName);
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(serviceName);
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setDescription("desc");
        mitigationActionMetadata.setToolName("cli");
        mitigationActionMetadata.setUser("user");
        request.setMitigationActionMetadata(mitigationActionMetadata);
        
        mitigationInfoHandler = spy(mitigationInfoHandler);
        Mockito.doReturn(99l).doReturn(100l).when(mitigationInfoHandler).getMaxWorkflowIdForDevice(
                eq(deviceName), eq(deviceScope), eq(null), isA(TSDMetrics.class));
        Mockito.doReturn(99).doReturn(100).when(mitigationInfoHandler)
                .getLatestVersionForMitigationOnDevice(eq(deviceName), eq(deviceScope),
                       eq(mitigationName), eq(null), isA(TSDMetrics.class));
        
        int requestVersion = 100;
        try {
            mitigationInfoHandler.storeUpdateMitigationRequest(request, mitigationDefinition,
                    requestVersion, RequestType.RollbackRequest, locations, tsdMetrics);
            fail();
        } catch (StaleRequestException400 ex) {
            String expectedMessageErrorPrefix = "Mitigation found in DDB for deviceName: BLACKWATCH_BORDER and deviceScope:"
                    + " GLOBAL and mitigationName: mitigation1 has a newer version: 100 than one in request: 100.";
            assertTrue(ex.getMessage().startsWith(expectedMessageErrorPrefix));
        }
    }
}
