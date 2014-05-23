package com.amazon.lookout.mitigation.service.workflow;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.activities.model.SchedulingStatus;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient;
import com.amazonaws.services.simpleworkflow.model.DescribeWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionDetail;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class SWFFailedWorkflowReaperTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    /**
     * Tests to run:
     * Test#1 - Happy case, where we find no workflows to reap.
     * Test#2 - Case where we find workflows to reap, but some of them have no incomplete instances.
     * Test#3 - Case where querying for active workflows throws an exception.
     * Test#4 - Case where querying for active workflows returns back a paginated response.
     * Test#5 - Case where querying for incomplete instances throws back an exception.
     * Test#6 - Case where querying for incomplete instances returns back a paginated response.
     * Test#7 - Case where some workflows have a null RunId, with a subset of them having been in that state for over threshold number of minutes.
     * Test#8 - Case where querying SWF for current execution status throws an exception for some workflows.
     * Test#9 - Case where updating status throws an exception for some workflows.
     */
    
    /**
     * Test#1 - Happy case, where we find no workflows to reap.
     */
    //@Test
    public void testNoWorkflowsToCleanup() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        QueryResult result = new QueryResult().withCount(0);
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result);
        
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
    }
    
    /**
     * Test#2 - Case where we find workflows to reap, but some of them have no incomplete instances.
     */
    //@Test
    public void testSomeWorkflowsWithNoIncompleteInstances() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2"));
        item.put("RunId", new AttributeValue().withS("RunId2"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("3"));
        item.put("RunId", new AttributeValue().withS("RunId3"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        QueryResult result = new QueryResult().withCount(items.size()).withItems(items);
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result);
        
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "1")).thenReturn(new ArrayList<String>());
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "2")).thenReturn(Lists.newArrayList("POP1"));
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "3")).thenReturn(Lists.newArrayList("POP1", "POP2"));
        
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class))).thenReturn(true);
        
        doNothing().when(reaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        // For Device#1 - 6 calls = 1 for workflowId1's status + 1 for workflowId2's status + 1 for workflowId2's instance +
        //                          1 for workflowId3's status + 2 for workflowId3's status
        // For all other devices, 3 calls => 1 for their workflow status + 2 for their instances' status.
        verify(reaper, times(6 + 3*(DeviceName.values().length - 1))).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test#3 - Case where querying for active workflows throws an exception.
     */
    //@Test
    public void testQueryingActiveWorkflowThrowsException() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenThrow(new RuntimeException());
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        verify(reaper, times(0)).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
    }
    
    /**
     * Test#4 - Case where querying for active workflows returns back a paginated response.
     */
    //@Test
    public void testPaginatedResponseForActiveWorkflowsQuery() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        QueryResult result1 = new QueryResult().withCount(items.size()).withItems(items).withLastEvaluatedKey(new HashMap<String, AttributeValue>());
        
        items = new ArrayList<>();
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2"));
        item.put("RunId", new AttributeValue().withS("RunId2"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("3"));
        item.put("RunId", new AttributeValue().withS("RunId3"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        QueryResult result2 = new QueryResult().withCount(items.size()).withItems(items);
        
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result1).thenReturn(result2);
        
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "1")).thenReturn(new ArrayList<String>());
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "2")).thenReturn(Lists.newArrayList("POP1"));
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "3")).thenReturn(Lists.newArrayList("POP1", "POP2"));
        
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class))).thenReturn(true);
        
        doNothing().when(reaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        verify(reaper, times(2 + (DeviceName.values().length - 1))).queryDynamoDB(any(QueryRequest.class)); // Only the first device gets 2 calls, everyone else gets 1
    }
    
    /**
     * Test#5 - Case where querying for incomplete mitigation instances throws an exception.
     */
    //@Test
    public void testQueryingIncompleteInstancesThrowsException() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2"));
        item.put("RunId", new AttributeValue().withS("RunId2"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        QueryResult result = new QueryResult().withCount(items.size()).withItems(items).withLastEvaluatedKey(new HashMap<String, AttributeValue>());
        
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result);
        
        when(reaper.queryNonCompletedInstancesForWorkflow(anyString(), anyString())).thenThrow(new RuntimeException());
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        verify(reaper, times(DeviceName.values().length)).queryNonCompletedInstancesForWorkflow(anyString(), anyString());
    }
    
    /**
     * Test#6 - Case where querying for incomplete mitigation instances returns back a paginated response.
     */
    //@Test
    public void testPaginatedResponseForIncompleteInstancesQuery() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        QueryResult activeWorkflowsResult = new QueryResult().withCount(items.size()).withItems(items);
        
        items = new ArrayList<>();
        item = new HashMap<String, AttributeValue>();
        item.put("SchedulingStatus", new AttributeValue().withS(SchedulingStatus.COMPLETED.name()));
        item.put("Location", new AttributeValue().withS("POP1"));
        items.add(item);
        QueryResult instancesQueryResult1 = new QueryResult().withCount(items.size()).withItems(items).withLastEvaluatedKey(new HashMap<String, AttributeValue>());
        
        items = new ArrayList<>();
        item = new HashMap<String, AttributeValue>();
        item.put("SchedulingStatus", new AttributeValue().withS(SchedulingStatus.RUNNING.name()));
        item.put("Location", new AttributeValue().withS("POP2"));
        items.add(item);
        QueryResult instancesQueryResult2 = new QueryResult().withCount(items.size()).withItems(items);
        
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(activeWorkflowsResult).thenReturn(instancesQueryResult1).thenReturn(instancesQueryResult2);
        
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class))).thenReturn(true);
        
        doNothing().when(reaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).queryNonCompletedInstancesForWorkflow(anyString(), anyString());
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        // For Device#1 - 3 query calls as configured above.
        // For all other devices, query 1 call (the last result in the mock query).
        verify(reaper, times(3 + (DeviceName.values().length - 1))).queryDynamoDB(any(QueryRequest.class));
    }
    
    /**
     * Test#7 - Case where some workflows have a null RunId, with a subset of them having been in that state for over threshold number of minutes.
     */
    //@Test
    public void testSomeWorkflowsWithNullRunId() {
        AmazonDynamoDBClient mockDDBClient = mock(AmazonDynamoDBClient.class);
        AmazonSimpleWorkflowClient mockSWFClient = mock(AmazonSimpleWorkflowClient.class);
        
        SWFFailedWorkflowReaper reaper = new SWFFailedWorkflowReaper(mockDDBClient, mockSWFClient, "beta", "TestSWFDomain", TestUtils.newNopMetricsFactory());
        SWFFailedWorkflowReaper spiedReaper = spy(reaper);

        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1")); // WorkflowId 1 has null swfRunId and has been in that state for over 30 minutes.
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2")); // WorkflowId 2 has null swfRunId and has been in that state for less than a minute.
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("3"));
        item.put("RunId", new AttributeValue().withS("RunId3")); // WorkflowId 3 has a swfRunId associated with it.
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        QueryResult result = new QueryResult().withCount(items.size()).withItems(items);
        doReturn(result).when(spiedReaper).queryDynamoDB(any(QueryRequest.class));
        
        doReturn(new ArrayList<String>()).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "1");
        doReturn(Lists.newArrayList("POP1")).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "2");
        doReturn(Lists.newArrayList("POP1", "POP2")).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "3");
        
        doNothing().when(spiedReaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        WorkflowExecutionInfo workflowExecutionInfo = new WorkflowExecutionInfo().withExecutionStatus("CLOSED").withCloseTimestamp(new Date(new DateTime(DateTimeZone.UTC).getMillis()));
        WorkflowExecutionDetail workflowDetail = new WorkflowExecutionDetail().withExecutionInfo(workflowExecutionInfo);
        when(mockSWFClient.describeWorkflowExecution(any(DescribeWorkflowExecutionRequest.class))).thenReturn(workflowDetail);
        
        Throwable caughtException = null;
        try {
            spiedReaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        // Only call the isWorkflowClosedInSWF for WorkflowId 3.
        verify(spiedReaper, times(1)).isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class));
        
        // Only call to update the WorkflowStatus for WorkflowId 1 (it has 0 instances, hence verifying only 1 call to updateStatusInDDB).
        verify(spiedReaper, times(1)).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test#8 - Case where querying SWF for current execution status throws an exception for some workflows.
     */
    //@Test
    public void testExceptionsForSomeSWFCalls() {
        AmazonDynamoDBClient mockDDBClient = mock(AmazonDynamoDBClient.class);
        AmazonSimpleWorkflowClient mockSWFClient = mock(AmazonSimpleWorkflowClient.class);
        
        SWFFailedWorkflowReaper reaper = new SWFFailedWorkflowReaper(mockDDBClient, mockSWFClient, "beta", "TestSWFDomain", TestUtils.newNopMetricsFactory());
        SWFFailedWorkflowReaper spiedReaper = spy(reaper);

        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2"));
        item.put("RunId", new AttributeValue().withS("RunId2"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("3"));
        item.put("RunId", new AttributeValue().withS("RunId3"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        QueryResult result = new QueryResult().withCount(items.size()).withItems(items);
        doReturn(result).when(spiedReaper).queryDynamoDB(any(QueryRequest.class));
        
        doReturn(new ArrayList<String>()).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "1");
        doReturn(Lists.newArrayList("POP1")).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "2");
        doReturn(Lists.newArrayList("POP1", "POP2")).when(spiedReaper).queryNonCompletedInstancesForWorkflow("POP_ROUTER", "3");
        
        doNothing().when(spiedReaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        WorkflowExecutionInfo workflowExecutionInfo2 = new WorkflowExecutionInfo().withExecutionStatus("CLOSED").withCloseTimestamp(new Date(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis()));
        WorkflowExecutionDetail workflowDetail2 = new WorkflowExecutionDetail().withExecutionInfo(workflowExecutionInfo2);
        
        WorkflowExecutionInfo workflowExecutionInfo3 = new WorkflowExecutionInfo().withExecutionStatus("CLOSED").withCloseTimestamp(new Date(new DateTime(DateTimeZone.UTC).getMillis()));
        WorkflowExecutionDetail workflowDetail3 = new WorkflowExecutionDetail().withExecutionInfo(workflowExecutionInfo3);

        // Throw exception for the first 3 calls (for WorkflowId 1) and return values for the subsequent calls.
        when(mockSWFClient.describeWorkflowExecution(any(DescribeWorkflowExecutionRequest.class))).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenThrow(new RuntimeException())
                                                                                                  .thenReturn(workflowDetail2).thenReturn(workflowDetail3);
        
        Throwable caughtException = null;
        try {
            spiedReaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        verify(spiedReaper, times(3)).isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class));
        
        // Only call to update the WorkflowStatus for WorkflowId 2 with 1 instances, hence verifying only 2 calls to updateStatusInDDB).
        verify(spiedReaper, times(2)).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test#9 - Case where updating status throws an exception for some workflows.
     */
    @Test
    public void testExceptionsWhenUpdatingStatus() {
        SWFFailedWorkflowReaper reaper = mock(SWFFailedWorkflowReaper.class);

        MetricsFactory metricsFactory = TestUtils.newNopMetricsFactory();
        when(reaper.getMetricsFactory()).thenReturn(metricsFactory);
        
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("1"));
        item.put("RunId", new AttributeValue().withS("RunId1"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("2"));
        item.put("RunId", new AttributeValue().withS("RunId2"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        item = new HashMap<String, AttributeValue>();
        item.put("WorkflowId", new AttributeValue().withN("3"));
        item.put("RunId", new AttributeValue().withS("RunId3"));
        item.put("RequestDate", new AttributeValue().withN(String.valueOf(new DateTime(DateTimeZone.UTC).minusMinutes(30).getMillis())));
        items.add(item);
        
        QueryResult result = new QueryResult().withCount(items.size()).withItems(items);
        when(reaper.queryDynamoDB(any(QueryRequest.class))).thenReturn(result);
        
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "1")).thenReturn(Lists.newArrayList("POP1", "POP2"));
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "2")).thenReturn(Lists.newArrayList("POP1", "POP2"));
        when(reaper.queryNonCompletedInstancesForWorkflow("POP_ROUTER", "3")).thenReturn(Lists.newArrayList("POP1", "POP2"));
        
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class))).thenReturn(true);
        
        // Fail the first call, hence failing updating the workflow status, succeed on rest of the calls.
        doThrow(new RuntimeException()).doNothing().when(reaper).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        
        when(reaper.isWorkflowClosedInSWF(anyString(), anyString(), anyString(), any(TSDMetrics.class))).thenReturn(true);
        
        doCallRealMethod().when(reaper).getSWFTerminatedWorkflowIds(anyList(), any(TSDMetrics.class));
        doCallRealMethod().when(reaper).reapActiveDDBWorkflowsTerminatedInSWF();
        
        Throwable caughtException = null;
        try {
            reaper.reapActiveDDBWorkflowsTerminatedInSWF();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        
        // For Device#1: 3 calls for WorkflowId 2 and 3 (both have 2 instances) = 6 calls + a failed call for updating instance status for WorkflowId 1 = 7 calls. 
        //               Note: We shouldn't be calling an update for workflow status for WorkflowId 1 since updating its instances status did not succeed.
        // For other devices, we would always return 2 instances, so a total of 3 updates per device.
        verify(reaper, times(7 + (DeviceName.values().length - 1)*3)).updateStatusInDDB(anyString(), anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }

}
