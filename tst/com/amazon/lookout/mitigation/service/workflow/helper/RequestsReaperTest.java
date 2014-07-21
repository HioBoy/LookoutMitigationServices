package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.ddb.model.MitigationRequestsModel;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.workflow.model.RequestToReap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.amazonaws.services.simpleworkflow.model.ExecutionStatus;
import com.amazonaws.services.simpleworkflow.model.ListClosedWorkflowExecutionsRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfos;
import com.google.common.collect.Lists;

public class RequestsReaperTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testCreateQueryRequest() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.createQueryForRequests(anyString(), anyMap())).thenCallRealMethod();
        when(reaper.getRequestsTableName()).thenReturn("MitigationRequests_Test");
        
        QueryRequest request = reaper.createQueryForRequests(DeviceName.POP_ROUTER.name(), null);
        assertNotNull(request);
        
        Map<String, Condition> keyConditions = request.getKeyConditions();
        assertNotNull(keyConditions);
        assertEquals(keyConditions.size(), 2);
        assertTrue(keyConditions.containsKey(MitigationRequestsModel.DEVICE_NAME_KEY));
        assertNotNull(keyConditions.get(MitigationRequestsModel.DEVICE_NAME_KEY));
        assertEquals(keyConditions.get(MitigationRequestsModel.DEVICE_NAME_KEY).getAttributeValueList().size(), 1);
        assertEquals(keyConditions.get(MitigationRequestsModel.DEVICE_NAME_KEY).getAttributeValueList().get(0).getS(), DeviceName.POP_ROUTER.name());
        assertEquals(keyConditions.get(MitigationRequestsModel.DEVICE_NAME_KEY).getComparisonOperator(), ComparisonOperator.EQ.name());
        
        assertTrue(keyConditions.containsKey(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY));
        assertNotNull(keyConditions.get(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY));
        assertEquals(keyConditions.get(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).getAttributeValueList().size(), 1);
        assertEquals(keyConditions.get(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).getAttributeValueList().get(0).getN(), "0");
        assertEquals(keyConditions.get(MitigationRequestsModel.UPDATE_WORKFLOW_ID_KEY).getComparisonOperator(), ComparisonOperator.EQ.name());
        
        Map<String, AttributeValue> lastEvaluationKey = request.getExclusiveStartKey();
        assertNull(lastEvaluationKey);
        
        String conditionalOperator = request.getConditionalOperator();
        assertNull(conditionalOperator);
        
        String indexName = request.getIndexName();
        assertNotNull(indexName);
        assertEquals(indexName, MitigationRequestsModel.UNEDITED_MITIGATIONS_LSI_NAME);
        
        boolean consistentRead = request.getConsistentRead();
        assertTrue(consistentRead);
        
        Map<String, Condition> queryFilters = request.getQueryFilter();
        assertNotNull(queryFilters);
        assertEquals(queryFilters.size(), 2);
        assertTrue(queryFilters.containsKey(MitigationRequestsModel.WORKFLOW_STATUS_KEY));
        assertNotNull(queryFilters.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY));
        assertEquals(queryFilters.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY).getAttributeValueList().size(), 1);
        assertEquals(queryFilters.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY).getAttributeValueList().get(0).getS(), WorkflowStatus.SUCCEEDED);
        assertEquals(queryFilters.get(MitigationRequestsModel.WORKFLOW_STATUS_KEY).getComparisonOperator(), ComparisonOperator.NE.name());
        
        assertTrue(queryFilters.containsKey(MitigationRequestsModel.REAPED_FLAG_KEY));
        assertNotNull(queryFilters.get(MitigationRequestsModel.REAPED_FLAG_KEY));
        assertEquals(queryFilters.get(MitigationRequestsModel.REAPED_FLAG_KEY).getAttributeValueList().size(), 1);
        assertEquals(queryFilters.get(MitigationRequestsModel.REAPED_FLAG_KEY).getAttributeValueList().get(0).getS(), "true");
        assertEquals(queryFilters.get(MitigationRequestsModel.REAPED_FLAG_KEY).getComparisonOperator(), ComparisonOperator.NE.name());
        
        String tableName = request.getTableName();
        assertEquals(tableName, "MitigationRequests_Test");
    }
    
    @Test
    public void testIsWorkflowClosedForActiveWorkflow() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        when(reaper.getSWFDomain()).thenReturn("TestDomain");
        
        AmazonSimpleWorkflowClient swfClient = mock(AmazonSimpleWorkflowClient.class);
        when(reaper.getSWFClient()).thenReturn(swfClient);
        
        WorkflowExecutionInfos workflowInfos = new WorkflowExecutionInfos();
        WorkflowExecutionInfo info = new WorkflowExecutionInfo();
        info.setExecutionStatus(ExecutionStatus.OPEN.name());
        workflowInfos.setExecutionInfos(Lists.newArrayList(info));
        when(swfClient.listClosedWorkflowExecutions(any(ListClosedWorkflowExecutionsRequest.class))).thenReturn(workflowInfos);
        
        boolean isWorkflowClosedInSWF = reaper.isWorkflowClosedInSWF(DeviceName.POP_ROUTER.name(), "1", TestUtils.newNopTsdMetrics());
        assertFalse(isWorkflowClosedInSWF);
    }
    
    @Test
    public void testIsWorkflowClosedForClosedWorkflow() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        when(reaper.getSWFDomain()).thenReturn("TestDomain");
        
        AmazonSimpleWorkflowClient swfClient = mock(AmazonSimpleWorkflowClient.class);
        when(reaper.getSWFClient()).thenReturn(swfClient);
        
        WorkflowExecutionInfos workflowInfos = new WorkflowExecutionInfos();
        WorkflowExecutionInfo info = new WorkflowExecutionInfo();
        info.setExecutionStatus(ExecutionStatus.CLOSED.name());
        info.setCloseTimestamp((new DateTime(DateTimeZone.UTC).minusMinutes(2)).toDate());
        workflowInfos.setExecutionInfos(Lists.newArrayList(info));
        when(swfClient.listClosedWorkflowExecutions(any(ListClosedWorkflowExecutionsRequest.class))).thenReturn(null).thenReturn(workflowInfos);
        
        boolean isWorkflowClosedInSWF = reaper.isWorkflowClosedInSWF(DeviceName.POP_ROUTER.name(), "1", TestUtils.newNopTsdMetrics());
        assertTrue(isWorkflowClosedInSWF);
        verify(swfClient, times(2)).listClosedWorkflowExecutions(any(ListClosedWorkflowExecutionsRequest.class));
    }
    
    @Test
    public void testQueryInstancesForWorkflow() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.createQueryForInstances(anyString(), anyString(), anyMap())).thenCallRealMethod();
        when(reaper.getInstancesTableName()).thenReturn("MitigationInstances_Test");
        
        QueryRequest request = reaper.createQueryForInstances(DeviceName.POP_ROUTER.name(), "1", null);
        assertNotNull(request);
        
        String deviceWorkflowId = MitigationInstancesModel.getDeviceWorkflowId(DeviceName.POP_ROUTER.name(), 1);
        
        Map<String, Condition> keyConditions = request.getKeyConditions();
        assertNotNull(keyConditions);
        assertEquals(keyConditions.size(), 1);
        assertTrue(keyConditions.containsKey(MitigationInstancesModel.DEVICE_WORKFLOW_KEY));
        assertNotNull(keyConditions.get(MitigationInstancesModel.DEVICE_WORKFLOW_KEY));
        assertEquals(keyConditions.get(MitigationInstancesModel.DEVICE_WORKFLOW_KEY).getAttributeValueList().size(), 1);
        assertEquals(keyConditions.get(MitigationInstancesModel.DEVICE_WORKFLOW_KEY).getAttributeValueList().get(0).getS(), deviceWorkflowId);
        assertEquals(keyConditions.get(MitigationInstancesModel.DEVICE_WORKFLOW_KEY).getComparisonOperator(), ComparisonOperator.EQ.name());
        
        Map<String, AttributeValue> lastEvaluationKey = request.getExclusiveStartKey();
        assertNull(lastEvaluationKey);
        
        String conditionalOperator = request.getConditionalOperator();
        assertNull(conditionalOperator);
        
        boolean consistentRead = request.getConsistentRead();
        assertTrue(consistentRead);
        
        List<String> attributesToGet = request.getAttributesToGet();
        assertEquals(attributesToGet.size(), 4);
        assertTrue(attributesToGet.contains(MitigationInstancesModel.SCHEDULING_STATUS_KEY));
        assertTrue(attributesToGet.contains(MitigationInstancesModel.LOCATION_KEY));
        assertTrue(attributesToGet.contains(MitigationInstancesModel.MITIGATION_STATUS_KEY));
        assertTrue(attributesToGet.contains(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        
        Map<String, Condition> queryFilters = request.getQueryFilter();
        assertNull(queryFilters);
        
        String tableName = request.getTableName();
        assertEquals(tableName, "MitigationInstances_Test");
    }
    
    @Test
    public void testQueryInstanceForWorkflow() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.createQueryForInstances(anyString(), anyString(), anyMap())).thenCallRealMethod();
        when(reaper.getInstancesTableName()).thenReturn("MitigationInstances_Test");
        when(reaper.queryInstancesForWorkflow(anyString(), anyString())).thenCallRealMethod();
        
        QueryResult result1 = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> itemDetails = new HashMap<>();
        itemDetails.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST1"));
        items.add(itemDetails);
        
        itemDetails = new HashMap<>();
        itemDetails.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST2"));
        items.add(itemDetails);
        result1.setItems(items);
        result1.setCount(items.size());
        result1.setLastEvaluatedKey(new HashMap<String, AttributeValue>());
        
        QueryResult result2 = new QueryResult();
        result2.setCount(0);
        
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result1).thenReturn(result2);
        
        Map<String, Map<String, AttributeValue>> instances = reaper.queryInstancesForWorkflow(DeviceName.POP_ROUTER.name(), "1");
        assertEquals(instances.size(), 2);
        assertTrue(instances.containsKey("TST1"));
        assertEquals(instances.get("TST1").get(MitigationInstancesModel.LOCATION_KEY).getS(), "TST1");
        assertTrue(instances.containsKey("TST2"));
        assertEquals(instances.get("TST2").get(MitigationInstancesModel.LOCATION_KEY).getS(), "TST2");
        
        verify(reaper, times(2)).queryDynamoDB(any(QueryRequest.class));
    }
    
    @Test
    public void testFilterInstancesToReap() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.filterInstancesToBeReaped(anyList(), anyMap())).thenCallRealMethod();
        
        Map<String, Map<String, AttributeValue>> instancesDetails = new HashMap<>();
        
        Map<String, AttributeValue> instanceTST2Info = new HashMap<>();
        instanceTST2Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("false"));
        instanceTST2Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.COMPLETED.name()));
        instanceTST2Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOY_SUCCEEDED));
        instancesDetails.put("TST2", instanceTST2Info);
        
        Map<String, AttributeValue> instanceTST3Info = new HashMap<>();
        instanceTST3Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("false"));
        instanceTST3Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.COMPLETED.name()));
        instanceTST3Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOYING));
        instancesDetails.put("TST3", instanceTST3Info);
        
        Map<String, AttributeValue> instanceTST4Info = new HashMap<>();
        instanceTST4Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("true"));
        instanceTST4Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.RUNNING.name()));
        instanceTST4Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOY_SUCCEEDED));
        instancesDetails.put("TST4", instanceTST4Info);
        
        Map<String, AttributeValue> instanceTST5Info = new HashMap<>();
        instanceTST5Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("true"));
        instanceTST5Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.COMPLETED.name()));
        instanceTST5Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOYING));
        instancesDetails.put("TST5", instanceTST5Info);
        
        Map<String, AttributeValue> instanceTST6Info = new HashMap<>();
        instanceTST6Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("true"));
        instanceTST6Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.COMPLETED.name()));
        instanceTST6Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOY_FAILED));
        instancesDetails.put("TST6", instanceTST6Info);
        
        Map<String, AttributeValue> instanceTST7Info = new HashMap<>();
        instanceTST7Info.put(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY, new AttributeValue("true"));
        instanceTST7Info.put(MitigationInstancesModel.SCHEDULING_STATUS_KEY, new AttributeValue(SchedulingStatus.COMPLETED.name()));
        instanceTST7Info.put(MitigationInstancesModel.MITIGATION_STATUS_KEY, new AttributeValue(MitigationStatus.DEPLOY_SUCCEEDED));
        instancesDetails.put("TST7", instanceTST7Info);
        
        Map<String, Map<String, AttributeValue>> instancesToReap = reaper.filterInstancesToBeReaped(Lists.newArrayList("TST1", "TST2", "TST3", "TST4", "TST5", "TST6", "TST7"), instancesDetails);
        assertEquals(instancesToReap.size(), 5);
        
        assertTrue(instancesToReap.containsKey("TST1"));
        assertNull(instancesToReap.get("TST1"));
        
        assertTrue(instancesToReap.containsKey("TST2"));
        assertEquals(instancesToReap.get("TST2").get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY), instanceTST2Info.get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        assertEquals(instancesToReap.get("TST2").get(MitigationInstancesModel.SCHEDULING_STATUS_KEY), instanceTST2Info.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY));
        assertEquals(instancesToReap.get("TST2").get(MitigationInstancesModel.MITIGATION_STATUS_KEY), instanceTST2Info.get(MitigationInstancesModel.MITIGATION_STATUS_KEY));
        
        assertTrue(instancesToReap.containsKey("TST3"));
        assertEquals(instancesToReap.get("TST3").get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY), instanceTST3Info.get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        assertEquals(instancesToReap.get("TST3").get(MitigationInstancesModel.SCHEDULING_STATUS_KEY), instanceTST3Info.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY));
        assertEquals(instancesToReap.get("TST3").get(MitigationInstancesModel.MITIGATION_STATUS_KEY), instanceTST3Info.get(MitigationInstancesModel.MITIGATION_STATUS_KEY));
        
        assertTrue(instancesToReap.containsKey("TST4"));
        assertEquals(instancesToReap.get("TST4").get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY), instanceTST4Info.get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        assertEquals(instancesToReap.get("TST4").get(MitigationInstancesModel.SCHEDULING_STATUS_KEY), instanceTST4Info.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY));
        assertEquals(instancesToReap.get("TST4").get(MitigationInstancesModel.MITIGATION_STATUS_KEY), instanceTST4Info.get(MitigationInstancesModel.MITIGATION_STATUS_KEY));
        
        assertTrue(instancesToReap.containsKey("TST5"));
        assertEquals(instancesToReap.get("TST5").get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY), instanceTST5Info.get(MitigationInstancesModel.ACTIVE_MITIGATIONS_UPDATED_KEY));
        assertEquals(instancesToReap.get("TST5").get(MitigationInstancesModel.SCHEDULING_STATUS_KEY), instanceTST5Info.get(MitigationInstancesModel.SCHEDULING_STATUS_KEY));
        assertEquals(instancesToReap.get("TST5").get(MitigationInstancesModel.MITIGATION_STATUS_KEY), instanceTST5Info.get(MitigationInstancesModel.MITIGATION_STATUS_KEY));
    }
    
    @Test
    public void testGetRequestsToReap() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        when(reaper.getRequestsToReap(any(TSDMetrics.class))).thenCallRealMethod();
        when(reaper.getMaxSecondsToStartWorkflow()).thenReturn(60);
        
        TSDMetrics metrics = mock(TSDMetrics.class);
        when(metrics.newSubMetrics(anyString())).thenReturn(metrics);
        
        QueryResult result1 = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        
        // Item1, running and started within the last few seconds.
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN("1"));
        item1.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(Lists.newArrayList("TST1", "TST2")));
        item1.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.RUNNING));
        item1.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusSeconds(10).getMillis())));
        item1.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue("TstMit1"));
        item1.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN("2"));
        item1.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue("TstMit1Template"));
        item1.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(DeviceScope.GLOBAL.name()));
        item1.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(RequestType.CreateRequest.name()));
        item1.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(ServiceName.Route53));
        item1.put(MitigationRequestsModel.RUN_ID_KEY, new AttributeValue("RandomRunID1"));
        items.add(item1);
        
        // Item2, status is running for a while, SWF API confirms that this workflow isn't closed as yet.
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN("2"));
        item2.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(Lists.newArrayList("TST1", "TST3")));
        item2.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.RUNNING));
        item2.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusSeconds(120).getMillis())));
        item2.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue("TstMit2"));
        item2.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN("1"));
        item2.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue("TstMit1Template"));
        item2.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(DeviceScope.GLOBAL.name()));
        item2.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(RequestType.DeleteRequest.name()));
        item2.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(ServiceName.Route53));
        item2.put(MitigationRequestsModel.RUN_ID_KEY, new AttributeValue("RandomRunID1"));
        items.add(item2);
        when(reaper.isWorkflowClosedInSWF(DeviceName.POP_ROUTER.name(), "1", metrics)).thenReturn(false);
        
        // Item3, status is running for a while, but SWF API confirms that this workflow is now marked as closed in SWF.
        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN("3"));
        item3.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(Lists.newArrayList("TST2", "TST3")));
        item3.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.RUNNING));
        item3.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusSeconds(120).getMillis())));
        item3.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue("TstMit3"));
        item3.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN("2"));
        item3.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue("TstMit3Template"));
        item3.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(DeviceScope.GLOBAL.name()));
        item3.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(RequestType.CreateRequest.name()));
        item3.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(ServiceName.Route53));
        item3.put(MitigationRequestsModel.RUN_ID_KEY, new AttributeValue("RandomRunID1"));
        items.add(item3);
        when(reaper.isWorkflowClosedInSWF(DeviceName.POP_ROUTER.name(), "3", metrics)).thenReturn(true);
        Map<String, Map<String, AttributeValue>> instancesDetails = new HashMap<>();
        Map<String, AttributeValue> info = new HashMap<>();
        info.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST2"));
        instancesDetails.put("TST2", info);
        info = new HashMap<>();
        info.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST3"));
        instancesDetails.put("TST3", info);
        when(reaper.queryInstancesForWorkflow(DeviceName.POP_ROUTER.name(), "3")).thenReturn(instancesDetails);
        when(reaper.filterInstancesToBeReaped(Lists.newArrayList("TST2", "TST3"), instancesDetails)).thenReturn(instancesDetails);
        
        
        // Item4, status is running for a while, but SWF API confirms that this workflow is now marked as closed in SWF. However, none of the instances need to be reaped.
        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN("4"));
        item4.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(Lists.newArrayList("TST1", "TST2", "TST3", "TST4")));
        item4.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.RUNNING));
        item4.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusSeconds(120).getMillis())));
        item4.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue("TstMit1"));
        item4.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN("5"));
        item4.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue("TstMit4Template"));
        item4.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(DeviceScope.GLOBAL.name()));
        item4.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(RequestType.DeleteRequest.name()));
        item4.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(ServiceName.Route53));
        item4.put(MitigationRequestsModel.RUN_ID_KEY, new AttributeValue("RandomRunID1"));
        items.add(item4);
        when(reaper.isWorkflowClosedInSWF(DeviceName.POP_ROUTER.name(), "4", metrics)).thenReturn(true);
        instancesDetails = new HashMap<>();
        instancesDetails.put("TST1", new HashMap<String, AttributeValue>());
        instancesDetails.put("TST2", new HashMap<String, AttributeValue>());
        instancesDetails.put("TST3", new HashMap<String, AttributeValue>());
        instancesDetails.put("TST4", new HashMap<String, AttributeValue>());
        when(reaper.queryInstancesForWorkflow(DeviceName.POP_ROUTER.name(), "4")).thenReturn(instancesDetails);
        when(reaper.filterInstancesToBeReaped(Lists.newArrayList("TST1", "TST2", "TST3", "TST4"), instancesDetails)).thenReturn(instancesDetails);
        
        result1.setItems(items);
        result1.setCount(items.size());
        result1.setLastEvaluatedKey(new HashMap<String, AttributeValue>());
        
        QueryResult result2 = new QueryResult();
        items = new ArrayList<>();
        // Item5, status is Indeterminate.
        Map<String, AttributeValue> item5 = new HashMap<>();
        item5.put(MitigationRequestsModel.WORKFLOW_ID_KEY, new AttributeValue().withN("5"));
        item5.put(MitigationRequestsModel.LOCATIONS_KEY, new AttributeValue().withSS(Lists.newArrayList("TST1", "TST2")));
        item5.put(MitigationRequestsModel.WORKFLOW_STATUS_KEY, new AttributeValue(WorkflowStatus.INDETERMINATE));
        item5.put(MitigationRequestsModel.REQUEST_DATE_IN_MILLIS_KEY, new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusSeconds(120).getMillis())));
        item5.put(MitigationRequestsModel.MITIGATION_NAME_KEY, new AttributeValue("TstMit1"));
        item5.put(MitigationRequestsModel.MITIGATION_VERSION_KEY, new AttributeValue().withN("2"));
        item5.put(MitigationRequestsModel.MITIGATION_TEMPLATE_NAME_KEY, new AttributeValue("TstMit1Template"));
        item5.put(MitigationRequestsModel.DEVICE_SCOPE_KEY, new AttributeValue(DeviceScope.GLOBAL.name()));
        item5.put(MitigationRequestsModel.REQUEST_TYPE_KEY, new AttributeValue(RequestType.CreateRequest.name()));
        item5.put(MitigationRequestsModel.SERVICE_NAME_KEY, new AttributeValue(ServiceName.Route53));
        item5.put(MitigationRequestsModel.RUN_ID_KEY, new AttributeValue("RandomRunID1"));
        items.add(item5);
        instancesDetails = new HashMap<>();
        info = new HashMap<>();
        info.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST1"));
        instancesDetails.put("TST1", info);
        info = new HashMap<>();
        info.put(MitigationInstancesModel.LOCATION_KEY, new AttributeValue("TST2"));
        instancesDetails.put("TST2", info);
        when(reaper.queryInstancesForWorkflow(DeviceName.POP_ROUTER.name(), "5")).thenReturn(instancesDetails);
        when(reaper.filterInstancesToBeReaped(Lists.newArrayList("TST1", "TST2"), instancesDetails)).thenReturn(instancesDetails);
        
        result2.setItems(items);
        result2.setCount(items.size());
        
        when(reaper.getUnsuccessfulUnreapedRequests(anyString(), anyMap())).thenReturn(result1).thenReturn(result2).thenReturn(null);
        
        List<RequestToReap> requestsToReap = reaper.getRequestsToReap(metrics);
        assertEquals(requestsToReap.size(), 3);
        
        RequestToReap request3 = requestsToReap.get(0);
        assertEquals(request3.getWorkflowIdStr(), item3.get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN());
        assertEquals(request3.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(request3.getDeviceScope(), DeviceScope.GLOBAL.name());
        assertEquals(request3.getLocationsToBeReaped().size(), 2);
        assertTrue(request3.getLocationsToBeReaped().containsKey("TST2"));
        assertTrue(request3.getLocationsToBeReaped().containsKey("TST3"));
        assertEquals(request3.getMitigationName(), "TstMit3");
        assertEquals(request3.getMitigationTemplate(), "TstMit3Template");
        assertEquals(request3.getMitigationVersion(), 2);
        assertEquals(request3.getRequestType(), RequestType.CreateRequest.name());
        assertEquals(request3.getServiceName(), ServiceName.Route53);
        assertEquals(request3.getWorkflowIdStr(), "3");
        
        RequestToReap request4 = requestsToReap.get(1);
        assertEquals(request4.getWorkflowIdStr(), item4.get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN());
        assertEquals(request4.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(request4.getDeviceScope(), DeviceScope.GLOBAL.name());
        assertEquals(request4.getLocationsToBeReaped().size(), 4);
        assertTrue(request4.getLocationsToBeReaped().containsKey("TST1"));
        assertTrue(request4.getLocationsToBeReaped().containsKey("TST2"));
        assertTrue(request4.getLocationsToBeReaped().containsKey("TST3"));
        assertTrue(request4.getLocationsToBeReaped().containsKey("TST4"));
        assertEquals(request4.getMitigationName(), "TstMit1");
        assertEquals(request4.getMitigationTemplate(), "TstMit4Template");
        assertEquals(request4.getMitigationVersion(), 5);
        assertEquals(request4.getRequestType(), RequestType.DeleteRequest.name());
        assertEquals(request4.getServiceName(), ServiceName.Route53);
        assertEquals(request4.getWorkflowIdStr(), "4");
        
        RequestToReap request5 = requestsToReap.get(2);
        assertEquals(request5.getWorkflowIdStr(), item5.get(MitigationRequestsModel.WORKFLOW_ID_KEY).getN());
        assertEquals(request5.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(request5.getDeviceScope(), DeviceScope.GLOBAL.name());
        assertEquals(request5.getLocationsToBeReaped().size(), 2);
        assertTrue(request5.getLocationsToBeReaped().containsKey("TST1"));
        assertTrue(request5.getLocationsToBeReaped().containsKey("TST2"));
        assertEquals(request5.getMitigationName(), "TstMit1");
        assertEquals(request5.getMitigationTemplate(), "TstMit1Template");
        assertEquals(request5.getMitigationVersion(), 2);
        assertEquals(request5.getRequestType(), RequestType.CreateRequest.name());
        assertEquals(request5.getServiceName(), ServiceName.Route53);
        assertEquals(request5.getWorkflowIdStr(), "5");
    }
    
    @Test
    public void testReapWorkflow() {
        RequestsReaper reaper = mock(RequestsReaper.class);
        
        TSDMetrics metrics = mock(TSDMetrics.class);
        when(metrics.newSubMetrics(anyString())).thenReturn(metrics);
        doCallRealMethod().when(reaper).reapRequests(metrics);
        
        SWFWorkflowStarter workflowStarter = mock(SWFWorkflowStarter.class);
        WorkflowClientExternal workflowClient = mock(WorkflowClientExternal.class);
        when(reaper.getWorkflowStarter()).thenReturn(workflowStarter);
        
        List<RequestToReap> requestsToReap = new ArrayList<>();
        RequestToReap requestToReap1 = new RequestToReap();
        requestToReap1.setWorkflowIdStr("1");
        requestToReap1.setDeviceName(DeviceName.POP_ROUTER.name());
        requestsToReap.add(requestToReap1);
        String swfWorkflowId1 = DeviceName.POP_ROUTER.name() + "_1_Reaper";
        when(workflowStarter.createReaperWorkflowClient(swfWorkflowId1, metrics)).thenReturn(workflowClient).thenThrow(new RuntimeException());
        
        RequestToReap requestToReap2 = new RequestToReap();
        requestToReap2.setWorkflowIdStr("2");
        requestToReap2.setDeviceName(DeviceName.POP_ROUTER.name());
        requestsToReap.add(requestToReap2);
        String swfWorkflowId2 = DeviceName.POP_ROUTER.name() + "_2_Reaper";
        when(workflowStarter.createReaperWorkflowClient(swfWorkflowId2, metrics)).thenReturn(workflowClient).thenThrow(new RuntimeException());
        
        RequestToReap requestToReap3 = new RequestToReap();
        requestToReap3.setWorkflowIdStr("3");
        requestToReap3.setDeviceName(DeviceName.POP_ROUTER.name());
        requestsToReap.add(requestToReap3);
        String swfWorkflowId3 = DeviceName.POP_ROUTER.name() + "_3_Reaper";
        when(workflowStarter.createReaperWorkflowClient(swfWorkflowId3, metrics)).thenReturn(workflowClient).thenThrow(new RuntimeException());
        
        when(reaper.getRequestsToReap(any(TSDMetrics.class))).thenReturn(requestsToReap);
        
        reaper.reapRequests(metrics);
        verify(workflowStarter, times(1)).startReaperWorkflow(swfWorkflowId1, requestToReap1, workflowClient, metrics);
        verify(workflowStarter, times(1)).startReaperWorkflow(swfWorkflowId2, requestToReap2, workflowClient, metrics);
        verify(workflowStarter, times(1)).startReaperWorkflow(swfWorkflowId3, requestToReap3, workflowClient, metrics);
    }
}
