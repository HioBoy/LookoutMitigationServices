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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.coral.google.common.collect.Sets;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class DDBBasedDeleteRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private final String domain = "beta";
    
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
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
     * Test the case where we evaluateActiveMitigationsForDDBQueryResult and successfully return a boolean flag that indicates that we've found the mitigation to be deleted.
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testEvaluateActiveMitigationsForDDBQueryResultHappyCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        request.setMitigationVersion(2);
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Map<String, AttributeValue> items = new HashMap<>();
        items.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, new AttributeValue(request.getMitigationName()));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(request.getMitigationVersion())));
        items.put(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, new AttributeValue(deviceNameAndScope.getDeviceScope().name()));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.SUCCEEDED));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, new AttributeValue().withN("10"));
        items.put(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, new AttributeValue("DeleteRequest"));
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(2).withItems(itemBuilder1.build()).withItems(itemBuilder2.build());
        
        boolean foundMitigationToDelete = false;
        Throwable caughtException = null;
        try {
            foundMitigationToDelete = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                                                  request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        assertTrue(foundMitigationToDelete);
    }
    
    /**
     * Test the case where we evaluateActiveMitigations and successfully return a boolean flag that indicates that we've found the mitigation to be deleted.
     * We don't expect any Exception to be thrown back.
     */
    @Test
    public void testEvaluateActiveMitigationsHappyCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        request.setMitigationVersion(2);
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        Map<String, AttributeValue> items = new HashMap<>();
        items.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, new AttributeValue(request.getMitigationName()));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, new AttributeValue().withN(String.valueOf(request.getMitigationVersion())));
        items.put(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, new AttributeValue(deviceNameAndScope.getDeviceScope().name()));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.SUCCEEDED));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, new AttributeValue().withN("10"));
        items.put(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, new AttributeValue("DeleteRequest"));
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(2).withItems(itemBuilder1.build()).withItems(itemBuilder2.build());
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
        
        boolean foundMitigationToDelete = false;
        Throwable caughtException = null;
        try {
            foundMitigationToDelete = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(),
                                                                                    request.getMitigationTemplate(), request.getMitigationVersion(), null, false, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        assertTrue(foundMitigationToDelete);
    }
    
    /**
     * Test the case where there already exists a successful request for deleting the same mitigation as the current request.
     * We should expect a DuplicateRequestException400 to be thrown back.
     */
    @Test
    public void testDuplicateDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder deleteItemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        DDBItemBuilder createItemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 1)
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(2).withItems(deleteItemBuilder.build(), createItemBuilder.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateRequestException400);
    }
    
    /**
     * Test the case where there already exists a successful request for deleting the same mitigation as the current request. However, the mitigation instance
     * deleted by this existing request was an old one. Since there is a newer create request after the previously successful delete request, the 
     * current delete request should be allowed.
     */
    @Test
    public void testCaseWhereDeleteRequestExistsButForAnOldCreateRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder deleteItemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        DDBItemBuilder create1ItemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                               .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 1)
                                                               .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        DDBItemBuilder create2ItemBuilder = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                                   .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                                   .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                                   .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                                   .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                                   .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 3)
                                                                   .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(3).withItems(create1ItemBuilder.build(), deleteItemBuilder.build(), create2ItemBuilder.build());
        
        boolean foundMitigationToDelete = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        assertTrue(foundMitigationToDelete);
    }
    
    /**
     * Test the case where there already exists a delete request for deleting the same mitigation as the current request, but is in a PartialSuccess state.
     * We should not throw back any exceptions in this case.
     */
    @Test
    public void testDuplicatePartialSuccessDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.PARTIAL_SUCCESS)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.DeleteRequest.name());
        
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        QueryResult queryResult = new QueryResult().withCount(2).withItems(itemBuilder1.build(), itemBuilder2.build());
        
        boolean foundMitigationToDelete = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                                                    request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        assertTrue(foundMitigationToDelete);
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
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
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
                                                                      request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof IllegalArgumentException);
    }
    
    /**
     * Test the case where we don't find any active mitigation corresponding to the delete request.
     * We should expect the return value to be false.
     */
    @Test
    public void testNoActiveMitigationToDelete() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        QueryResult queryResult = new QueryResult().withCount(0).withItems(Collections.EMPTY_LIST);
        boolean foundMitigationToDelete = storageHandler.evaluateActiveMitigationsForDDBQueryResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                                                                    request.getMitigationName(), request.getMitigationTemplate(), request.getMitigationVersion(), tsdMetrics);
        assertFalse(foundMitigationToDelete);
    }
    
    /**
     * Test the happy case for getting the max workflowId from the existing workflows in DDB.
     */
    @Test
    public void testGettingMaxWorkflowId() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        long workflowIdToReturn = (long) 3;
        
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withNumericAttribute("WorkflowId", workflowIdToReturn);
        QueryResult result = new QueryResult().withCount(1).withItems(itemBuilder.build()).withItems(itemBuilder.build()).withCount(1);
        
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);
        
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, null, tsdMetrics);
        
        assertEquals((long) workflowId, workflowIdToReturn);
        verify(storageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
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
        
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation3")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 2)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 34)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());
        
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, request.getMitigationName())
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, request.getMitigationVersion())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 27)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.EditRequest.name());
        
        DDBItemBuilder itemBuilder3 = new DDBItemBuilder().withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, "Mitigation1")
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.MITIGATION_VERSION_KEY, 1)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, request.getMitigationTemplate())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, deviceNameAndScope.getDeviceScope().name())
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, WorkflowStatus.SUCCEEDED)
                                                          .withNumericAttribute(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, 20)
                                                          .withStringAttribute(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, RequestType.CreateRequest.name());

        QueryResult queryResult1 = new QueryResult().withCount(1).withItems(itemBuilder1.build()).withLastEvaluatedKey(new HashMap<String, AttributeValue>());
        QueryResult queryResult2 = new QueryResult().withCount(2).withItems(itemBuilder2.build()).withItems(itemBuilder3.build());
        doReturn(queryResult1).doReturn(queryResult2).when(spiedStorageHandler).queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class));
        
        long maxWorkflowId = spiedStorageHandler.getMaxWorkflowIdForDevice(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), null, tsdMetrics);

        assertEquals(maxWorkflowId, 34);
        verify(spiedStorageHandler, times(0)).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
        verify(spiedStorageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case when we try to get max workflowId for the device, but there are no currently active mitigations for this device.
     * We expect a null to be returned in this case.
     */
    @Test
    public void testGetMaxWorkflowIdWhenNoActiveMitigationsForDevice() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(new QueryResult().withCount(0));
        
        when(storageHandler.getKeysForActiveMitigationsForDevice(anyString())).thenCallRealMethod();
        when(storageHandler.getKeysForDeviceAndWorkflowId(anyString(), anyLong())).thenCallRealMethod();
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, null, tsdMetrics);
        assertNull(workflowId);
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
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
        
        boolean foundMitigationToDelete = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
                                                                                              request.getMitigationTemplate(), request.getMitigationVersion(), null, false, tsdMetrics);
        
        assertFalse(foundMitigationToDelete);
        
        verify(spiedStorageHandler, times(1)).getKeysForActiveMitigationsForDevice(deviceNameAndScope.getDeviceName().name());
        verify(spiedStorageHandler, times(0)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
        
        reset(spiedStorageHandler);
        doReturn(queryResult).when(spiedStorageHandler).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
        
        // Now check the case where we pass the maxWorkflowId from the last run.
        foundMitigationToDelete = spiedStorageHandler.evaluateActiveMitigations(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), request.getMitigationName(), 
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
        
        doReturn((long) 12).when(spiedStorageHandler).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doReturn(true).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doThrow(new RuntimeException()).when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class),
                any(MitigationDefinition.class), anySet(), any(DeviceNameAndScope.class), anyLong(), 
                any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
            spiedStorageHandler.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().startsWith(DDBBasedDeleteRequestStorageHandler.DELETE_REQUEST_STORAGE_FAILED_LOG_PREFIX));
        
        verify(spiedStorageHandler, times(DDBBasedDeleteRequestStorageHandler.DDB_ACTIVITY_MAX_ATTEMPTS))
                .storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                        anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
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
        
        doReturn(maxWorkflowId).when(spiedStorageHandler).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doReturn(true).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                anySet(), any(DeviceNameAndScope.class), anyLong(), 
                any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Long newWorkflowId = null;
        newWorkflowId = spiedStorageHandler.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), tsdMetrics);
        
        assertNotNull(newWorkflowId);
        assertEquals((long) newWorkflowId, maxWorkflowId + 1);
        
        verify(spiedStorageHandler, times(3)).storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        String deviceName = deviceNameAndScope.getDeviceName().name();
        String deviceScope = deviceNameAndScope.getDeviceScope().name();
        String mitigationName = request.getMitigationName();
        String mitigationTemplate = request.getMitigationTemplate();
        int mitigationVersion = request.getMitigationVersion();
        TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedDeleteRequestStorageHandler.storeRequestForWorkflow");
        
        verify(spiedStorageHandler, times(1)).evaluateActiveMitigations(deviceName, deviceScope, mitigationName, mitigationTemplate, mitigationVersion, null, false, subMetrics);
        verify(spiedStorageHandler, times(2)).evaluateActiveMitigations(deviceName, deviceScope, mitigationName, mitigationTemplate, mitigationVersion, maxWorkflowId, true, subMetrics);
    }
    
    /**
     * Test the case where we get a null for max workflowId.
     * We expect an exception to be thrown back in this case.
     */
    @Test
    public void testNoWorkflowIdForStoringDeleteRequest() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        DDBBasedDeleteRequestStorageHandler storageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        DDBBasedDeleteRequestStorageHandler spiedStorageHandler = spy(storageHandler);
        
        DeleteMitigationFromAllLocationsRequest request = createDeleteMitigationRequest();
        
        doReturn(null).when(spiedStorageHandler).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doReturn(true).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doNothing().when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
            spiedStorageHandler.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof RuntimeException);
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
        
        doReturn(maxWorkflowId).when(spiedStorageHandler).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doReturn(true).when(spiedStorageHandler).evaluateActiveMitigations(anyString(), anyString(), anyString(), anyString(), anyInt(), anyLong(), anyBoolean(), any(TSDMetrics.class));
        doReturn((long) 10).when(spiedStorageHandler).getSleepMillisMultiplierBetweenStoreRetries();
        doNothing().when(spiedStorageHandler).storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        Long newWorkflowId = null;
        newWorkflowId = spiedStorageHandler.storeRequestForWorkflow(request, Sets.newHashSet("TST1"), tsdMetrics);
        
        assertNotNull(newWorkflowId);
        assertEquals((long) newWorkflowId, maxWorkflowId + 1);
        
        verify(spiedStorageHandler, times(1)).storeRequestInDDB(any(MitigationModificationRequest.class), any(MitigationDefinition.class),
                anySet(), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
    }

}
