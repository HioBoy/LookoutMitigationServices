package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class DDBBasedRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    /**
     * Test the happy case where the StorageHandler is able to store everything just fine into DDB.
     */
    @Test
    public void testHappyCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        
        assertNull(caughtException);
        verify(storageHandler, times(1)).putItemInDDB(anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where the StorageHandler throws transient exceptions, but succeeds on retrying.
     */
    @Test
    public void testTransientFailuresCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(storageHandler).putItemInDDB(anyMap(), any(TSDMetrics.class));
        when(storageHandler.getSleepMillisMultiplierOnPutRetry()).thenReturn(1);
        
        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        
        assertNull(caughtException);
        verify(storageHandler, times(3)).putItemInDDB(anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where the StorageHandler throws transient exceptions when storing and never succeeds in any of the
     * DDB_PUT_ITEM_MAX_ATTEMPTS number of attempts.
     * We expect an exception to the be thrown in this case.
     */
    @Test
    public void testStorageFailuresCase() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        long workflowId = 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        doCallRealMethod().when(storageHandler).storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        when(storageHandler.generateAttributesToStore(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion)).thenCallRealMethod();
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        doThrow(new RuntimeException()).when(storageHandler).putItemInDDB(anyMap(), any(TSDMetrics.class));
        when(storageHandler.getSleepMillisMultiplierOnPutRetry()).thenReturn(1);
        
        Throwable caughtException = null;
        try {
            storageHandler.storeRequestInDDB(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        
        assertNotNull(caughtException);
        verify(storageHandler, times(3)).putItemInDDB(anyMap(), any(TSDMetrics.class));
    }
    
    @Test
    public void testGenerateAttributesToStore() {
        DDBBasedRequestStorageHandler storageHandler = mock(DDBBasedRequestStorageHandler.class);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        request.setLocation(Lists.newArrayList("POP1", "POP2"));
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        Long workflowId = (long) 1;
        RequestType requestType = RequestType.CreateRequest;
        int mitigationVersion = 1;
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        when(storageHandler.getJSONDataConverter()).thenReturn(jsonDataConverter);
        when(storageHandler.generateAttributesToStore(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), anyString(), anyInt())).thenCallRealMethod();
        
        Map<String, AttributeValue> attributesToStore = storageHandler.generateAttributesToStore(request, deviceNameAndScope, workflowId, requestType.name(), mitigationVersion);
        
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
        assertEquals(workflowStatus, WorkflowStatus.SCHEDULED);
        
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
        assertEquals(locations, request.getLocation());
        
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
}
