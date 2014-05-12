package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.BlastRadiusCheck;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.template.Route53SingleCustomerMitigationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.ServiceTemplateValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
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
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class DDBBasedCreateRequestStorageHandlerTest {
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    private final String domain = "beta";
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    public static MitigationDefinition defaultCreateMitigationDefinition() {
        return createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
    }
    
    public static MitigationDefinition createMitigationDefinition(String attrName, List<String> attrValues) {
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(attrName);
        constraint.setAttributeValues(attrValues);
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        return definition;
    }
    
    public static CreateMitigationRequest generateCreateMitigationRequest() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("TestMitigationName");
        request.setMitigationTemplate(MitigationTemplate.Router_RateLimit_Route53Customer);
        request.setServiceName(ServiceName.Route53);
        
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser("lookout");
        metadata.setToolName("lookoutui");
        metadata.setDescription("why not?");
        metadata.setRelatedTickets(Lists.newArrayList("12345"));
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = defaultCreateMitigationDefinition();
        request.setMitigationDefinition(definition);
        
        BlastRadiusCheck check1 = new BlastRadiusCheck();
        DateTime now = new DateTime();
        check1.setEndDateTime(now.toString());
        check1.setStartDateTime(now.minusHours(1).toString());
        check1.setFailureThreshold(5.0);
        
        BlastRadiusCheck check2 = new BlastRadiusCheck();
        check2.setEndDateTime(now.toString());
        check2.setStartDateTime(now.minusHours(2).toString());
        check2.setFailureThreshold(10.0);
        
        List<MitigationDeploymentCheck> checks = new ArrayList<>();
        checks.add(check1);
        checks.add(check2);
        request.setPreDeploymentChecks(checks);
        
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
    
    private class MockTemplateBasedRequestValidator extends TemplateBasedRequestValidator {
        private final ServiceTemplateValidator serviceTemplateValidator;
        public MockTemplateBasedRequestValidator(ServiceSubnetsMatcher serviceSubnetsMatcher, ServiceTemplateValidator serviceTemplateValidator) {
            super(serviceSubnetsMatcher);
            this.serviceTemplateValidator = serviceTemplateValidator;
        }
        
        @Override
        protected ServiceTemplateValidator getValidator(String templateName) {
            return serviceTemplateValidator;
        }
    }
    
    /**
     * Test the case where the new mitigation has the same mitigation name as an existing one.
     * We should expect an IllegalArgumentException to be thrown back.
     */
    @Test
    public void testDuplicateMitigationName() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        MitigationModificationRequest request = generateCreateMitigationRequest();
        MitigationDefinition newDefinition = defaultCreateMitigationDefinition();
        String newDefinitionJsonString = jsonDataConverter.toData(newDefinition);
        int newDefinitionHashcode = newDefinitionJsonString.hashCode();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        
        Map<String, AttributeValue> items = new HashMap<>();
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_ID_KEY, new AttributeValue().withN("10"));
        items.put(DDBBasedRequestStorageHandler.DEVICE_SCOPE_KEY, new AttributeValue(deviceNameAndScope.getDeviceScope().name()));
        items.put(DDBBasedRequestStorageHandler.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.SUCCEEDED));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_DEFINITION_KEY, new AttributeValue(existingDefinitionJsonString));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_NAME_KEY, new AttributeValue(request.getMitigationName()));
        items.put(DDBBasedRequestStorageHandler.MITIGATION_TEMPLATE_KEY, new AttributeValue("Some other template"));
        items.put(DDBBasedRequestStorageHandler.REQUEST_TYPE_KEY, new AttributeValue("CreateRequest"));
        
        QueryResult queryResult = mock(QueryResult.class);
        when(queryResult.getItems()).thenReturn(Lists.newArrayList(items));
        
        Throwable caughtException = null;
        try {
            storageHandler.checkForDuplicatesFromDDBResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, 
                                                           newDefinition, newDefinitionHashcode, request.getMitigationName(), request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateRequestException400);
    }
    
    /**
     * Test the case where the checkDuplicateDefinition method checks the definitions and finds out no duplicates.
     */
    @Test
    public void testCheckDuplicateDefinitionHappyCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        MitigationDefinition newDefinition = defaultCreateMitigationDefinition();
        String newDefinitionJsonString = jsonDataConverter.toData(newDefinition);
        int newDefinitionHashcode = newDefinitionJsonString.hashCode();
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        
        Throwable caughtException = null;
        try {
            storageHandler.checkDuplicateDefinition(existingDefinitionJsonString, "ExistingName", "ExistingTemplate", "NewName", "NewTemplate", newDefinition, newDefinitionHashcode, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        verify(templateBasedValidator, times(1)).validateCoexistenceForTemplateAndDevice(anyString(), anyString(), any(MitigationDefinition.class), anyString(), anyString(), any(MitigationDefinition.class));
    }
    
    /**
     * Test the case where the checkDuplicateDefinition method finds 2 definitions with the same hashcode, but with different actual definitions.
     * We don't expect any exceptions to be thrown in this case.
     */
    @Test
    public void testCheckDuplicateDefinitionWithSameHashcodeButDifferentStrings() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        MitigationDefinition newDefinition = defaultCreateMitigationDefinition();
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        
        int newDefinitionHashcode = existingDefinitionJsonString.hashCode();
        
        Throwable caughtException = null;
        try {
            storageHandler.checkDuplicateDefinition(existingDefinitionJsonString, "ExistingName", "ExistingTemplate", "NewName", "NewTemplate", newDefinition, newDefinitionHashcode, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        verify(templateBasedValidator, times(1)).validateCoexistenceForTemplateAndDevice(anyString(), anyString(), any(MitigationDefinition.class), anyString(), anyString(), any(MitigationDefinition.class));
    }
    
    /**
     * Test the case where the checkDuplicateDefinition method checks the definitions and finds out duplicates.
     * We expect a DuplicateDefinition exception to be thrown in this case.
     */
    @Test
    public void testCheckDuplicateDefinitionForDuplicateDefinitions() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        MitigationDefinition newDefinition = defaultCreateMitigationDefinition();
        String newDefinitionJsonString = jsonDataConverter.toData(newDefinition);
        int newDefinitionHashcode = newDefinitionJsonString.hashCode();
        
        Throwable caughtException = null;
        try {
            storageHandler.checkDuplicateDefinition(newDefinitionJsonString, "ExistingName", "ExistingTemplate", "NewName", "NewTemplate", newDefinition, newDefinitionHashcode, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
        verify(templateBasedValidator, times(0)).validateCoexistenceForTemplateAndDevice(anyString(), anyString(), any(MitigationDefinition.class), anyString(), anyString(), any(MitigationDefinition.class));
    }
    
    /**
     * Test the case where the checkDuplicateDefinition method checks the definitions and finds out they aren't duplicates of each other but they cannot coexist.
     * We expect a DuplicateDefinition exception to be thrown in this case.
     */
    @Test
    public void testCheckDuplicateDefinitionForNonCoexistentDefinitions() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        ServiceSubnetsMatcher subnetsMatcher = mock(ServiceSubnetsMatcher.class);
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(subnetsMatcher);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        MitigationDefinition newDefinition = defaultCreateMitigationDefinition();
        String newDefinitionJsonString = jsonDataConverter.toData(newDefinition);
        int newDefinitionHashcode = newDefinitionJsonString.hashCode();
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        
        Throwable caughtException = null;
        try {
            storageHandler.checkDuplicateDefinition(existingDefinitionJsonString, "ExistingName", MitigationTemplate.Router_RateLimit_Route53Customer, "NewName", 
                                                    MitigationTemplate.Router_RateLimit_Route53Customer, newDefinition, newDefinitionHashcode, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
    }
    
    /**
     * Test the happy case for getting the max workflowId from the existing workflows in DDB.
     */
    @Test
    public void testHappyCaseForGettingMaxWorkflowId() {
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
     * Test if we use the maxWorkflowId seen in the previous request appropriately for querying for the max workflowIds in DDB.
     */
    @Test
    public void testGettingMaxWorkflowIdUsingPrevValue() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        Long workflowIdToReturn = (long) 3;
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withNumericAttribute("WorkflowId", workflowIdToReturn);
        QueryResult result = new QueryResult().withCount(1).withItems(itemBuilder.build()).withItems(itemBuilder.build()).withCount(1);
        when(storageHandler.queryDynamoDB(any(QueryRequest.class), any(TSDMetrics.class))).thenReturn(result);
        
        when(storageHandler.getKeysForActiveMitigationsForDevice(anyString())).thenCallRealMethod();
        when(storageHandler.getKeysForDeviceAndWorkflowId(anyString(), anyLong())).thenCallRealMethod();
        
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        Long workflowId = storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, workflowIdToReturn, tsdMetrics);
        
        assertEquals(workflowId, workflowIdToReturn);
        verify(storageHandler, times(1)).getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).getKeysForDeviceAndWorkflowId(deviceName, workflowIdToReturn);
        verify(storageHandler, times(0)).getKeysForActiveMitigationsForDevice(anyString());
    }
    
    /**
     * Test the case where we are unable to fetch the currently active mitigations for a device from DDB.
     * We expect an exception to be thrown back in this case.
     */
    @Test
    public void testGetMaxWorkflowIdOnActiveMitigationsRetrievalFailure() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        
        when(storageHandler.getKeysForActiveMitigationsForDevice(anyString())).thenCallRealMethod();
        when(storageHandler.getKeysForDeviceAndWorkflowId(anyString(), anyLong())).thenCallRealMethod();
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        Throwable caughtException = null;
        try {
            when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
            storageHandler.getMaxWorkflowIdForDevice(deviceName, deviceScope, null, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        verify(storageHandler, times(1)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
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
     * Test the case when we check and find duplicate definitions for the device.
     * We expect an exception to be thrown back in this case.
     */
    @Test
    public void testDuplicateDefinitions() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        MitigationDefinition definition = request.getMitigationDefinition();
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String definitionAsJsonString = jsonDataConverter.toData(definition);
        int definitionHashcode = definitionAsJsonString.hashCode();
        
        String mitigationName = request.getMitigationName();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        String existingMitigationTemplate1 = "RandomTemplate1";
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 2)
                                                         .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName1")
                                                         .withStringAttribute("MitigationTemplate", existingMitigationTemplate1).withStringAttribute("MitigationDefinition", existingDefinitionJsonString)
                                                         .withStringAttribute("RequestType", "CreateRequest");
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("key1", new AttributeValue("value1"));
        QueryResult result1 = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(lastEvaluatedKey);
        
        String existingMitigationTemplate2 = "RandomTemplate2";
        itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 3)
                                          .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName2")
                                          .withStringAttribute("MitigationTemplate", existingMitigationTemplate2).withStringAttribute("MitigationDefinition", definitionAsJsonString)
                                          .withStringAttribute("RequestType", "CreateRequest");
        QueryResult result2 = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(null);
                
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenReturn(result1).thenReturn(result2);
        doCallRealMethod().when(storageHandler).queryAndCheckDuplicateMitigations(anyString(), anyString(), any(MitigationDefinition.class), anyInt(), 
                                                                                  anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        
        Map<String, ServiceTemplateValidator> serviceTemplateValidators = new HashMap<>();
        Route53SingleCustomerMitigationValidator route53Validator = new Route53SingleCustomerMitigationValidator(serviceSubnetsMatcher);
        serviceTemplateValidators.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53Validator);
        serviceTemplateValidators.put(existingMitigationTemplate1, route53Validator);
        serviceTemplateValidators.put(existingMitigationTemplate2, route53Validator);
        
        TemplateBasedRequestValidator templateBasedValidator = new MockTemplateBasedRequestValidator(serviceSubnetsMatcher, route53Validator);

        when(storageHandler.getTemplateBasedValidator()).thenReturn(templateBasedValidator);
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageHandler.queryAndCheckDuplicateMitigations(deviceName, deviceScope, definition, definitionHashcode, mitigationName, 
                                                             existingMitigationTemplate2, null, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
        verify(storageHandler, times(2)).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), 
                                                                         anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        verify(storageHandler, times(2)).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        verify(storageHandler, times(2)).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case when we check and find no duplicate definitions for the device and use the maxWorkflowId from the previous query result.
     * We don't expect an exception to be thrown back in this case.
     */
    @Test
    public void testNonDuplicateDefinitionsWithWorkflowIdFromPreviousAttempt() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        int newDefinitionHashcode = jsonDataConverter.toData(request.getMitigationDefinition()).hashCode();
        
        MitigationDefinition definition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String definitionAsJsonString = jsonDataConverter.toData(definition);
        
        String mitigationName = request.getMitigationName();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        String existingMitigationTemplate = "RandomTemplate";
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 3)
                                                           .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName2")
                                                           .withStringAttribute("MitigationTemplate", existingMitigationTemplate).withStringAttribute("MitigationDefinition", definitionAsJsonString)
                                                           .withStringAttribute("RequestType", "CreateRequest");
        QueryResult result = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(null);
                
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenReturn(result);
        doCallRealMethod().when(storageHandler).queryAndCheckDuplicateMitigations(anyString(), anyString(), any(MitigationDefinition.class), anyInt(), 
                                                                                  anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        
        Map<String, ServiceTemplateValidator> serviceTemplateValidators = new HashMap<>();
        Route53SingleCustomerMitigationValidator route53Validator = new Route53SingleCustomerMitigationValidator(serviceSubnetsMatcher);
        serviceTemplateValidators.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53Validator);
        serviceTemplateValidators.put(existingMitigationTemplate, route53Validator);
        
        TemplateBasedRequestValidator templateBasedValidator = new MockTemplateBasedRequestValidator(serviceSubnetsMatcher, route53Validator);

        when(storageHandler.getTemplateBasedValidator()).thenReturn(templateBasedValidator);
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageHandler.queryAndCheckDuplicateMitigations(deviceName, deviceScope, request.getMitigationDefinition(), newDefinitionHashcode, mitigationName, existingMitigationTemplate, (long) 2, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        verify(storageHandler, times(1)).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), 
                                                                         anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
        verify(storageHandler, times(0)).getKeysForActiveMitigationsForDevice(anyString());
    }
    
    /**
     * Test the case where we query DDB for checking for duplicate definitions, but receive a few transient failures.
     * We don't expect any exception to be thrown back and expect getting back the results after retrying.
     */
    @Test
    public void testCheckDuplicateDefinitionsWithTransientQueryFailures() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        int newDefinitionHashcode = jsonDataConverter.toData(request.getMitigationDefinition()).hashCode();
        
        MitigationDefinition definition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String definitionAsJsonString = jsonDataConverter.toData(definition);
        
        String mitigationName = request.getMitigationName();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        String existingMitigationTemplate = "RandomTemplate";
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 3)
                                                           .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName2")
                                                           .withStringAttribute("MitigationTemplate", existingMitigationTemplate).withStringAttribute("MitigationDefinition", definitionAsJsonString)
                                                           .withStringAttribute("RequestType", "CreateRequest");
        QueryResult result = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(null);
                
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenReturn(result);
        doCallRealMethod().when(storageHandler).queryAndCheckDuplicateMitigations(anyString(), anyString(), any(MitigationDefinition.class), anyInt(), 
                                                                                  anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        
        Map<String, ServiceTemplateValidator> serviceTemplateValidators = new HashMap<>();
        Route53SingleCustomerMitigationValidator route53Validator = new Route53SingleCustomerMitigationValidator(serviceSubnetsMatcher);
        serviceTemplateValidators.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53Validator);
        serviceTemplateValidators.put(existingMitigationTemplate, route53Validator);
        
        TemplateBasedRequestValidator templateBasedValidator = new MockTemplateBasedRequestValidator(serviceSubnetsMatcher, route53Validator);

        when(storageHandler.getTemplateBasedValidator()).thenReturn(templateBasedValidator);
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageHandler.queryAndCheckDuplicateMitigations(deviceName, deviceScope, request.getMitigationDefinition(), newDefinitionHashcode, mitigationName, existingMitigationTemplate, (long) 2, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        verify(storageHandler, times(1)).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), 
                                                                         anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
        verify(storageHandler, times(0)).getKeysForActiveMitigationsForDevice(anyString());
        verify(storageHandler, times(3)).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where we query DDB for checking for duplicate definitions, but receive chronic failures.
     * We expect an exception to be thrown back.
     */
    @Test
    public void testCheckDuplicateDefinitionsWithChronicQueryFailures() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        int newDefinitionHashcode = jsonDataConverter.toData(request.getMitigationDefinition()).hashCode();
        
        String mitigationName = request.getMitigationName();
        
        String existingMitigationTemplate = "RandomTemplate";
                
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        doCallRealMethod().when(storageHandler).queryAndCheckDuplicateMitigations(anyString(), anyString(), any(MitigationDefinition.class), anyInt(), 
                                                                                  anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        
        Map<String, ServiceTemplateValidator> serviceTemplateValidators = new HashMap<>();
        Route53SingleCustomerMitigationValidator route53Validator = new Route53SingleCustomerMitigationValidator(serviceSubnetsMatcher);
        serviceTemplateValidators.put(MitigationTemplate.Router_RateLimit_Route53Customer, route53Validator);
        serviceTemplateValidators.put(existingMitigationTemplate, route53Validator);
        
        TemplateBasedRequestValidator templateBasedValidator = new MockTemplateBasedRequestValidator(serviceSubnetsMatcher, route53Validator);

        when(storageHandler.getTemplateBasedValidator()).thenReturn(templateBasedValidator);
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageHandler.queryAndCheckDuplicateMitigations(deviceName, deviceScope, request.getMitigationDefinition(), newDefinitionHashcode, mitigationName, existingMitigationTemplate, (long) 2, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof RuntimeException);
        verify(storageHandler, times(0)).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), 
                                                                         anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        verify(storageHandler, times(0)).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).getKeysForDeviceAndWorkflowId(anyString(), anyLong());
        verify(storageHandler, times(0)).getKeysForActiveMitigationsForDevice(anyString());
        verify(storageHandler, times(DDBBasedCreateRequestStorageHandler.DDB_ACTIVITY_MAX_ATTEMPTS)).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case when we try to get max workflowId for the device and we find an existing definition which isn't compatible with the new definitions
     * for the template being used.
     * We expect an exception to be thrown back in this case.
     */
    @Test
    public void testNonCompatibleDefinitionsForTemplate() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        String deviceName = DeviceName.POP_ROUTER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        MitigationDefinition definition = request.getMitigationDefinition();
        JsonDataConverter jsonDataConverter = new JsonDataConverter();
        String definitionAsJsonString = jsonDataConverter.toData(definition);
        int definitionHashcode = definitionAsJsonString.hashCode();
        
        String mitigationName = request.getMitigationName();
        String mitigationTemplate = request.getMitigationTemplate();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString = jsonDataConverter.toData(existingDefinition);
        DDBItemBuilder itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 3)
                                                         .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName1")
                                                         .withStringAttribute("MitigationTemplate", request.getMitigationTemplate()).withStringAttribute("MitigationDefinition", existingDefinitionJsonString)
                                                         .withStringAttribute("RequestType", "CreateRequest");
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("key1", new AttributeValue("value1"));
        QueryResult result1 = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(lastEvaluatedKey);
        
        itemBuilder = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withNumericAttribute("WorkflowId", 3)
                                          .withStringAttribute("WorkflowStatus", "SCHEDULED").withStringAttribute("MitigationName", "RandomName1")
                                          .withStringAttribute("MitigationTemplate", "RandomTemplate1").withStringAttribute("MitigationDefinition", existingDefinitionJsonString)
                                          .withStringAttribute("RequestType", "CreateRequest");
        QueryResult result2 = new QueryResult().withCount(1).withItems(itemBuilder.build()).withLastEvaluatedKey(null);
                
        Long workflowIdToReturn = (long) 3;
        when(storageHandler.getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class))).thenReturn(result1).thenReturn(result2);
        doCallRealMethod().when(storageHandler).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        doCallRealMethod().when(storageHandler).queryAndCheckDuplicateMitigations(anyString(), anyString(), any(MitigationDefinition.class), anyInt(), 
                                                                                  anyString(), anyString(), anyLong(), any(TSDMetrics.class));
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        ServiceSubnetsMatcher serviceSubnetsMatcher = mock(ServiceSubnetsMatcher.class);
        TemplateBasedRequestValidator templateBasedValidator = new TemplateBasedRequestValidator(serviceSubnetsMatcher);

        when(storageHandler.getTemplateBasedValidator()).thenReturn(templateBasedValidator);
        when(storageHandler.getKeysForActiveMitigationsForDevice(anyString())).thenCallRealMethod();
        when(storageHandler.getKeysForDeviceAndWorkflowId(anyString(), anyLong())).thenCallRealMethod();
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            storageHandler.queryAndCheckDuplicateMitigations(deviceName, deviceScope, definition, definitionHashcode, mitigationName, mitigationTemplate, null, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
        Mockito.verify(storageHandler, times(1)).checkForDuplicatesFromDDBResult(anyString(), anyString(), any(QueryResult.class), any(MitigationDefinition.class), 
                                                                            anyInt(), anyString(), anyString(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).checkDuplicateDefinition(anyString(), anyString(), anyString(), anyString(), anyString(), any(MitigationDefinition.class), anyInt(), any(TSDMetrics.class));
        verify(storageHandler, times(1)).getActiveMitigationsForDevice(anyString(), anyString(), anySet(), anyMap(), anyMap(), anyString(), anyMap(), any(TSDMetrics.class));
        verify(storageHandler, times(0)).getKeysForDeviceAndWorkflowId(deviceName, workflowIdToReturn);
        verify(storageHandler, times(1)).getKeysForActiveMitigationsForDevice(anyString());
    }
    
    /**
     * Test the checkDuplicatesAndGetMaxWorkflowId method for dissimilar definitions.
     * We expect the check to succeed and don't expect any exceptions to be thrown in this case.
     */
    @Test
    public void testCheckDuplicatesForNonDuplicateDefinitions() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        MitigationDefinition definition = request.getMitigationDefinition();
        String mitigationDefinitionAsJsonString = new JsonDataConverter().toData(definition);
        int newDefinitionHashcode = mitigationDefinitionAsJsonString.hashCode();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString1 = new JsonDataConverter().toData(existingDefinition);
        
        long existingWorkflowId1 = 1;
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withStringAttribute("WorkflowStatus", "SUCCESSFUL")
                                                          .withStringAttribute("MitigationName", "Mitigation1").withStringAttribute("MitigationTemplate", request.getMitigationTemplate())
                                                          .withStringAttribute("MitigationDefinition", existingDefinitionJsonString1).withNumericAttribute("WorkflowId", existingWorkflowId1)
                                                          .withStringAttribute("RequestType", "CreateRequest");
        
        existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_PORT.name(), Lists.newArrayList("53"));
        String existingDefinitionJsonString2 = new JsonDataConverter().toData(existingDefinition);
        long existingWorkflowId2 = 2;
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withStringAttribute("WorkflowStatus", "SCHEDULED")
                                                          .withStringAttribute("MitigationName", "Mitigation2").withStringAttribute("MitigationTemplate", request.getMitigationTemplate())
                                                          .withStringAttribute("MitigationDefinition", existingDefinitionJsonString2).withNumericAttribute("WorkflowId", existingWorkflowId2)
                                                          .withStringAttribute("RequestType", "CreateRequest");
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder1.build()).withItems(itemBuilder2.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.checkForDuplicatesFromDDBResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, request.getMitigationDefinition(), 
                                                           newDefinitionHashcode, request.getMitigationName(), request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test the checkDuplicatesAndGetMaxWorkflowId method for duplicate definitions.
     * We expect the check to fail and an exceptions to be thrown.
     */
    @Test
    public void testCheckDuplicatesForDuplicateDefinitions() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        TemplateBasedRequestValidator templateBasedValidator = mock(TemplateBasedRequestValidator.class);
        DDBBasedCreateRequestStorageHandler storageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        MitigationDefinition definition = request.getMitigationDefinition();
        String mitigationDefinitionAsJsonString = new JsonDataConverter().toData(definition);
        int newDefinitionHashcode = mitigationDefinitionAsJsonString.hashCode();
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        MitigationDefinition existingDefinition = createMitigationDefinition(PacketAttributesEnumMapping.SOURCE_IP.name(), Lists.newArrayList("1.2.3.4"));
        String existingDefinitionJsonString1 = new JsonDataConverter().toData(existingDefinition);
        
        long existingWorkflowId1 = 1;
        DDBItemBuilder itemBuilder1 = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withStringAttribute("WorkflowStatus", "SUCCESSFUL")
                                                          .withStringAttribute("MitigationName", "Mitigation1").withStringAttribute("MitigationTemplate", request.getMitigationTemplate())
                                                          .withStringAttribute("MitigationDefinition", existingDefinitionJsonString1).withNumericAttribute("WorkflowId", existingWorkflowId1)
                                                          .withStringAttribute("RequestType", "CreateRequest");
        
        long existingWorkflowId2 = 2;
        DDBItemBuilder itemBuilder2 = new DDBItemBuilder().withStringAttribute("DeviceScope", deviceNameAndScope.getDeviceScope().name()).withStringAttribute("WorkflowStatus", "SCHEDULED")
                                                          .withStringAttribute("MitigationName", "Mitigation2").withStringAttribute("MitigationTemplate", request.getMitigationTemplate())
                                                          .withStringAttribute("MitigationDefinition", mitigationDefinitionAsJsonString).withNumericAttribute("WorkflowId", existingWorkflowId2)
                                                          .withStringAttribute("RequestType", "CreateRequest");
        
        QueryResult queryResult = new QueryResult().withCount(1).withItems(itemBuilder1.build()).withItems(itemBuilder2.build());
        
        Throwable caughtException = null;
        try {
            storageHandler.checkForDuplicatesFromDDBResult(deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(), queryResult, request.getMitigationDefinition(), 
                                                              newDefinitionHashcode, request.getMitigationName(), request.getMitigationTemplate(), tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof DuplicateDefinitionException400);
    }
    
    /**
     * Test the storeRequestForWorkflow method where it succeeds in storing this request into the table.
     */
    @Test
    public void testStoreRequestForWorkflowHappyCase() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        long maxWorkflowId = 3;
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(maxWorkflowId);
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
        long workflowId = storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        assertEquals(workflowId, maxWorkflowId + 1);
    }
    
    /**
     * Test the storeRequestForWorkflow method where it gets a null when checking for maxWorkflowId for existing mitigations (this would happen if no mitigations exist for this device).
     * storeRequestForWorkflow method should succeed in storing this request into the table for this case.
     */
    @Test
    public void testStoreRequestForWorkflowForNullMaxWorkflowId() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(null);
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
        long workflowId = storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        assertEquals(workflowId, deviceNameAndScope.getDeviceScope().getMinWorkflowId());
    }
    
    /**
     * Test the storeRequestForWorkflow method where it gets a maxWorkflowId for existing mitigations that is the max value associated with this deviceName and deviceScope.
     * storeRequestForWorkflow method should throw back an exception for this case.
     */
    @Test
    public void testStoreRequestForWorkflowForOutOfBoundsWorkflowId() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        long maxWorkflowId = deviceNameAndScope.getDeviceScope().getMaxWorkflowId();
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(maxWorkflowId);
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        Throwable caughtException = null;
        try {
            when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
            doCallRealMethod().when(storageHandler).sanityCheckWorkflowId(anyLong(), any(DeviceNameAndScope.class));
            storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the storeRequestForWorkflow method where it fails when checking for maxWorkflowId for existing mitigations.
     * storeRequestForWorkflow method should throw back an exception for this case.
     */
    @Test
    public void testStoreRequestForWorkflowWhereGetMaxWorkflowIdFails() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        
        Throwable caughtException = null;
        try {
            when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
            storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the storeRequestForWorkflow method where it fails to persist the request into DDB.
     * storeRequestForWorkflow method should throw back an exception for this case.
     */
    @Test
    public void testStoreRequestForWorkflowWhereStorageFails() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        long maxWorkflowId = 3;
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(maxWorkflowId);
        
        doThrow(new RuntimeException()).when(storageHandler).storeRequestInDDB(any(CreateMitigationRequest.class), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        Throwable caughtException = null;
        try {
            when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
            storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the storeRequestForWorkflow method where it fails to persist the request into DDB for a few attempts, but should succeed on a subsequent attempt.
     */
    @Test
    public void testStoreRequestForWorkflowWithTransientStorageFailures() {
        DDBBasedCreateRequestStorageHandler storageHandler = mock(DDBBasedCreateRequestStorageHandler.class);
        
        CreateMitigationRequest request = generateCreateMitigationRequest();
        
        long maxWorkflowId = 3;
        when(storageHandler.getMaxWorkflowIdForDevice(anyString(), anyString(), anyLong(), any(TSDMetrics.class))).thenReturn(maxWorkflowId);
        
        doThrow(new RuntimeException()).doThrow(new RuntimeException()).doThrow(new RuntimeException()).doNothing().when(storageHandler).storeRequestInDDB(any(CreateMitigationRequest.class), any(DeviceNameAndScope.class), anyLong(), any(RequestType.class), anyInt(), any(TSDMetrics.class));
        
        when(storageHandler.getJSONDataConverter()).thenReturn(new JsonDataConverter());
        
        when(storageHandler.storeRequestForWorkflow(any(CreateMitigationRequest.class), any(TSDMetrics.class))).thenCallRealMethod();
        long workflowId = storageHandler.storeRequestForWorkflow(request, tsdMetrics);
        assertEquals(workflowId, maxWorkflowId + 1);
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        String deviceName = deviceNameAndScope.getDeviceName().name();
        String deviceScope = deviceNameAndScope.getDeviceScope().name();
        MitigationDefinition mitigationDefinition = request.getMitigationDefinition();
        JsonDataConverter dataConverter = new JsonDataConverter();
        String mitigationDefinitionAsJSONString = dataConverter.toData(mitigationDefinition);
        int mitigationDefinitionHash = mitigationDefinitionAsJSONString.hashCode();
        TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedCreateRequestStorageHandler.storeRequestForWorkflow");
        
        verify(storageHandler, times(1)).queryAndCheckDuplicateMitigations(deviceName, deviceScope, mitigationDefinition, mitigationDefinitionHash, request.getMitigationName(), 
                                                                           request.getMitigationTemplate(), null, subMetrics);
        verify(storageHandler, times(3)).queryAndCheckDuplicateMitigations(deviceName, deviceScope, mitigationDefinition, mitigationDefinitionHash, request.getMitigationName(), 
                                                                           request.getMitigationTemplate(), maxWorkflowId, subMetrics);
    }
}
