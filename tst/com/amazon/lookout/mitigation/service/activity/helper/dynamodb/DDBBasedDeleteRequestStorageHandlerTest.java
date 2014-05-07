package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Lists;
import com.mallardsoft.tuple.Pair;

@SuppressWarnings("unchecked")
public class DDBBasedDeleteRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private final String domain = "beta";
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    public static DeleteMitigationFromAllLocationsRequest createDeleteMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("TestMitigationName");
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        request.setServiceName(ServiceName.Route53);
        request.setMitigationVersion(1);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser("lookout");
        metadata.setToolName("lookoutui");
        metadata.setDescription("why not?");
        metadata.setRelatedTickets(Lists.newArrayList("12345"));
        request.setMitigationActionMetadata(metadata);
        
        return request;
    }
    
    private class DDBItemBuilder {
        private final Map<String, AttributeValue> item = new HashMap<>();
        
        public DDBItemBuilder withStringAttribute(String attributeName, String attributeValue) {
            item.put(attributeName, new AttributeValue(attributeValue));
            return this;
        }
        
        public DDBItemBuilder withNumericAttribute(String attributeName, Number attributeValue) {
            item.put(attributeName, new AttributeValue().withN(String.valueOf(attributeValue)));
            return this;
        }
        
        public Map<String, AttributeValue> build() {
            return this.item;
        }
    }
    
    /**
     * Test the case where we evaluateActiveMitigationsForDDBQueryResult and successfully return the max workflowId and the boolean flag that indicates that we've found the mitigation to be deleted.
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testEvaluateActiveMitigationsHappyCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Map<String, AttributeValue> items = new HashMap<>();
        items.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, new AttributeValue(request.getMitigationName()));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(request.getMitigationVersion())));
        items.put(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, new AttributeValue(deviceNameAndScope.getDeviceScope().name()));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.SUCCEEDED));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, new AttributeValue().withN("10"));
        items.put(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, new AttributeValue("DeleteRequest"));
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation1")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());

        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation2")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 4)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        DDBItemBuilder itemBuilder3 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation3")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 29)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(3).withItems(itemBuilder1.build()).withItems(itemBuilder2.build()).withItems(itemBuilder3.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where we have a similar delete request, however they differ for the device scopes - so both of those requests are valid.
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testSameDeletesForDifferentDeviceScopes() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                         .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, "SomeOtherDeviceScope")
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                         .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());

        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where we have a similar delete request, however the request in DDB has been updated (probably because it failed).
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testSameDeletesForAnUpdatedRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                         .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                         .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                         .withNumericAttribute(DDBBasedRequestStorageHandler.UPDATE_WORKFLOW_ID_KEY, 7)
                                                         .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());

        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where there exists an identical request for deleting the same mitigation as the current request, however the previous request has failed status.
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testSimilarDeleteRequestWithFailedStatus() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.FAILED)
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the case where there already exists a successful request for deleting the same mitigation as the current request.
     * We should expect an IllegalArgumentException to be thrown back.
     */
    @Test
    public void testDuplicateDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    /**
     * Test the case where we find an active version of the mitigation to be deleted, but with a higher version number than the one passed in the delete request.
     * We should expect an IllegalArgumentException to be thrown back.
     */
    @Test
    public void testDeleteOlderVersion() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion() + 1)
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    /**
     * Test the case where we find an active version of the mitigation to be deleted, but which was created with a different template than the one specified by the delete request.
     * We should expect an IllegalArgumentException to be thrown back.
     */
    @Test
    public void testDeleteMitigationWithDifferentTemplate() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, "RandomTemplate")
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                           .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                           .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    /**
     * Test the case where we get the correct max workflowId, but don't find any active mitigation corresponding to the delete request.
     * We should expect the return value (Pair<Long, Boolean>) with the maxWorkflowId and false for the boolean flag.
     */
    @Test
    public void testGettingMaxWorkflowIdButNoActiveMitigationToDelete() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation1")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());

        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 4)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, "OtherScope")
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());

        DDBItemBuilder itemBuilder3 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation3")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 34)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(3).withItems(itemBuilder1.build()).withItems(itemBuilder2.build()).withItems(itemBuilder3.build());
        
        Pair<Long, Boolean> evalResult = null;
        evalResult = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                                 request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        assertNotNull(evalResult);
        long maxWorkflowId = Pair.get1(evalResult);
        assertEquals(maxWorkflowId, 34);
        
        boolean foundMitigationToDelete = Pair.get2(evalResult);
        assertFalse(foundMitigationToDelete);
    }
    
    /**
     * Test the case where we get the correct max workflowId and set the flag that we have found an active mitigation corresponding to the delete request.
     * We should expect the return value (Pair<Long, Boolean>) with the maxWorkflowId and true for the boolean flag.
     */
    @Test
    public void testGettingMaxWorkflowIdAndActiveMitigationToDelete() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation1")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());

        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());

        DDBItemBuilder itemBuilder3 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation3")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 34)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(3).withItems(itemBuilder1.build()).withItems(itemBuilder2.build()).withItems(itemBuilder3.build());
        
        Pair<Long, Boolean> evalResult = null;
        evalResult = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                               request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), false, tsdMetrics);
        assertNotNull(evalResult);
        
        long maxWorkflowId = Pair.get1(evalResult);
        assertEquals(maxWorkflowId, 34);
        
        boolean foundMitigationToDelete = Pair.get2(evalResult);
        assertTrue(foundMitigationToDelete);
    }
    
    /**
     * Test the case where we get the correct max workflowId and set the flag that we have found an active mitigation corresponding to the delete request, 
     * only after going through the paginated DDB Query response.
     * We should expect the return value (Pair<Long, Boolean>) with the maxWorkflowId and true for the boolean flag.
     */
    @Test
    public void testGettingMaxWorkflowIdAndActiveMitigationAfterPaginatedResponse() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation1")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());

        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());

        DDBItemBuilder itemBuilder3 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation3")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 34)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        QueryResult queryResult1 = new QueryResult().withCount(1).withItems(itemBuilder1.build()).withLastEvaluatedKey(new HashMap<String, AttributeValue>());
        QueryResult queryResult2 = new QueryResult().withCount(2).withItems(itemBuilder2.build()).withItems(itemBuilder3.build());
        doReturn(queryResult1).doReturn(queryResult2).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anySet(), anyMap(), anyMap(), anyString(), any(TSDMetrics.class));
        
        Pair<Long, Boolean> evalResult = null;
        evalResult = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
                                                                   request.getMitigationTemplate(), request.getMitigationVersion(), null, false, tsdMetrics);
        
        assertNotNull(evalResult);
        long maxWorkflowId = Pair.get1(evalResult);
        assertEquals(maxWorkflowId, 34);
        
        boolean foundMitigationToDelete = Pair.get2(evalResult);
        assertTrue(foundMitigationToDelete);
        
        verify(spiedStorageHandler, times(2)).getActiveMitigationsForDevice(anyString(), anySet(), anyMap(), anyMap(), anyString(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where we get no active mitigations, causing the evaluation to return null maxWorkflowId and false for the flag indicating if we found the active mitigation to delete.
     * We should expect the return value (Pair<Long, Boolean>) with the maxWorkflowId set to null and false for the boolean flag.
     */
    @Test
    public void testNullMaxWorkflowIdForNoActiveMitigationsCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        QueryResult queryResult = new QueryResult().withCount(0);
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anySet(), anyMap(), anyMap(), anyString(), any(TSDMetrics.class));
        
        Pair<Long, Boolean> evalResult = null;
        evalResult = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
                                                                   request.getMitigationTemplate(), request.getMitigationVersion(), null, false, tsdMetrics);
        
        assertNotNull(evalResult);
        Long maxWorkflowId = Pair.get1(evalResult);
        assertNull(maxWorkflowId);
        
        boolean foundMitigationToDelete = Pair.get2(evalResult);
        assertFalse(foundMitigationToDelete);
    }
    
    /**
     * Test if we query DDB with the correct keys when evaluating active mitigation.
     */
    @Test
    public void testQueryingUsingCorrectKeys() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        QueryResult queryResult = new QueryResult().withCount(0);
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anySet(), anyMap(), anyMap(), anyString(), any(TSDMetrics.class));
        
        Pair<Long, Boolean> evalResult = null;
        evalResult = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
                                                                   request.getMitigationTemplate(), request.getMitigationVersion(), null, false, tsdMetrics);
        
        assertNotNull(evalResult);
        Long maxWorkflowId = Pair.get1(evalResult);
        assertNull(maxWorkflowId);
        
        boolean foundMitigationToDelete = Pair.get2(evalResult);
        assertFalse(foundMitigationToDelete);
        
        verify(spiedStorageHandler, times(1)).getKeysForActiveMitigationsForDevice(deviceNameAndScope.getDeviceName().name());
        verify(spiedStorageHandler, times(0)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
        
        reset(spiedStorageHandler);
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anySet(), anyMap(), anyMap(), anyString(), any(TSDMetrics.class));
        
        // Now check the case where we pass the maxWorkflowId from the last run.
        evalResult = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
                                                                      request.getMitigationTemplate(), request.getMitigationVersion(), (long) 9, false, tsdMetrics);

        verify(spiedStorageHandler, times(0)).getKeysForActiveMitigationsForDevice(deviceNameAndScope.getDeviceName().name());
        verify(spiedStorageHandler, times(1)).getKeysForDeviceAndWorkflowId(deviceNameAndScope.getDeviceName().name(), (long) 9);
    }
    
    /**
     * Test that we throw an exception if storing delete request in DDB fails after all retries.
     */
    @Test
    public void testHandlingFailureToStoreDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        
        doReturn(Pair.from((long) 5, true)).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doThrow(new RuntimeException()).when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), 
        																			any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
            spiedStorageHandler.storeRequestForWorkflow(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().startsWith(DDBBasedDeleteRequestStorageHandler.DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX));
        
        verify(spiedStorageHandler, times(DDBBasedDeleteRequestStorageHandler.NEW_WORKFLOW_ID_MAX_ATTEMPTS)).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class),
                                                                                                                               anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where the storeRequestInDDB fails for a few attempts, but succeeds on retries.
     */
    @Test
    public void testRetriesOnFailureToStoreDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        long maxWorkflowId = 5;
        
        doReturn(Pair.from(maxWorkflowId, true)).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), 
        																														any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Long newWorkflowId = null;
        newWorkflowId = spiedStorageHandler.storeRequestForWorkflow(request, tsdMetrics);
        
        assertNotNull(newWorkflowId);
        assertEquals((long) newWorkflowId, maxWorkflowId + 1);
        
        verify(spiedStorageHandler, times(3)).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
    }
    
    /**
     * Test the happy case where we are able to get the max workflowId and find an existing active mitigation to be deleted.
     */
    @Test
    public void testHappyCaseForStoringDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        long maxWorkflowId = 5;
        
        doReturn(Pair.from(maxWorkflowId, true)).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doNothing().when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Long newWorkflowId = null;
        newWorkflowId = spiedStorageHandler.storeRequestForWorkflow(request, tsdMetrics);
        
        assertNotNull(newWorkflowId);
        assertEquals((long) newWorkflowId, maxWorkflowId + 1);
        
        verify(spiedStorageHandler, times(1)).storeRequestInDDB(any(MitigationModificationRequest.class), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
    }

}
