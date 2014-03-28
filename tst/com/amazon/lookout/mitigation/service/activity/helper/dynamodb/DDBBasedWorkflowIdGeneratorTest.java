package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.mitigations.MitigationTemplate;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;

public class DDBBasedWorkflowIdGeneratorTest {
    
    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @Before
    public void setUpOnce() {
        TestUtils.configure();
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    /**
     * Test the normal case where the workflowId can be updated right away in DDB for getting the new workflowId.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testGenerateWorkflowIdHappyCase() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        
        UpdateItemResult result = new UpdateItemResult();
        Map<String, AttributeValue> updatedValues = new HashMap<>();
        updatedValues.put(DDBBasedWorkflowIdGenerator.STATUS_KEY, new AttributeValue("LOCKED"));
        
        long workflowIdInResponse = 5;
        AttributeValue workflowIdInResponseAttributeValue = new AttributeValue();
        workflowIdInResponseAttributeValue.setN(String.valueOf(workflowIdInResponse));
        updatedValues.put(DDBBasedWorkflowIdGenerator.WORKFLOW_ID_KEY, workflowIdInResponseAttributeValue);
        result.setAttributes(updatedValues);
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenReturn(result);
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        long workflowId = generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        assertEquals(workflowId, workflowIdInResponse);
    }
    
    /**
     * Test the case where a new workflowId is requested for a non-existent mitigation template.
     * We should expect an exception to be throw in this case.
     */
    //@Test
    public void testGenerateWorkflowIdBadTemplateCase() {
        AmazonDynamoDBClient dynamoDBClient = mock(AmazonDynamoDBClient.class);
        String testDomain = "beta";
        DDBBasedWorkflowIdGenerator generator = new DDBBasedWorkflowIdGenerator(dynamoDBClient, testDomain);
        Throwable caughtException = null;
        try {
            generator.generateWorkflowId("BadTemplate", tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where we might get failures on some calls, but we should retry until the threshold attempts to get the result.
     * In this test we fail twice, but the third time we should expect back results. 
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testRetryOnSomeFailures() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        UpdateItemResult result = new UpdateItemResult();
        Map<String, AttributeValue> updatedValues = new HashMap<>();
        updatedValues.put(DDBBasedWorkflowIdGenerator.STATUS_KEY, new AttributeValue("LOCKED"));
        
        long workflowIdInResponse = 5;
        AttributeValue workflowIdInResponseAttributeValue = new AttributeValue();
        workflowIdInResponseAttributeValue.setN(String.valueOf(workflowIdInResponse));
        updatedValues.put(DDBBasedWorkflowIdGenerator.WORKFLOW_ID_KEY, workflowIdInResponseAttributeValue);
        result.setAttributes(updatedValues);
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException())
                                                                                            .thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"))
                                                                                            .thenReturn(result);
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();

        long workflowId = generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        assertEquals(workflowId, workflowIdInResponse);
    }
    
    /**
     * Test the case where we might get failures on all DDB calls. We should expect an exception to be thrown back. 
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testFailureToUpdate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"));
        
        when(generator.getItemBlockingUpdate(anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case where we might get failures on update DDB calls + we have the blockingItem the first time we get ConditionalCheckFailedException
     * but after all our update attempts we cannot verify if we are blocked on the same item as before. 
     * In this case we expect an exception to be thrown back.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testFailureToIdentifyForceUpdate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"));
        
        Map<String, AttributeValue> blockingItem = new HashMap<>();
        when(generator.getItemBlockingUpdate(anyMap(), any(TSDMetrics.class))).thenReturn(blockingItem).thenThrow(new RuntimeException());
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        verify(generator, never()).forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where we might get failures on update DDB calls + we have the blockingItem the first time we get ConditionalCheckFailedException
     * and after all our update attempts we verify that we are blocked on the same item as before, but the force update fails. 
     * In this case we expect an exception to be thrown back.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testFailureToForceUpdate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException())
                                                                                            .thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"));
        
        Map<String, AttributeValue> blockingItem = new HashMap<>();
        when(generator.getItemBlockingUpdate(anyMap(), any(TSDMetrics.class))).thenReturn(blockingItem).thenReturn(blockingItem);
        
        when(generator.forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        verify(generator, times(1)).forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where we might get failures on update DDB calls + we have the blockingItem the first time we get ConditionalCheckFailedException
     * and after all our update attempts we notice that we are now blocked on a different item as before. 
     * In this case we expect an exception to be thrown back.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testFailureBlockedUpdate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"));
        
        Map<String, AttributeValue> blockingItem = new HashMap<>();
        Map<String, AttributeValue> newBlockingItem = new HashMap<>();
        newBlockingItem.put("key1", new AttributeValue("value1"));
        when(generator.getItemBlockingUpdate(anyMap(), any(TSDMetrics.class))).thenReturn(blockingItem).thenReturn(newBlockingItem);
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        try {
            generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        verify(generator, never()).forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where we might get failures on update DDB calls + we have the blockingItem the first time we get ConditionalCheckFailedException
     * and after all our update attempts we are still blocked on the same item as before. 
     * In this case we expect a forced update.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testForceUpdate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        when(generator.getUpdateRetrySleepMillis()).thenReturn(1);
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException())
                                                                                            .thenThrow(new ConditionalCheckFailedException("expectedConditionNotSatisfied"));
        
        Map<String, AttributeValue> blockingItem = new HashMap<>();
        when(generator.getItemBlockingUpdate(anyMap(), any(TSDMetrics.class))).thenReturn(blockingItem).thenReturn(blockingItem);
        
        UpdateItemResult result = new UpdateItemResult();
        Map<String, AttributeValue> updatedValues = new HashMap<>();
        updatedValues.put(DDBBasedWorkflowIdGenerator.STATUS_KEY, new AttributeValue("LOCKED"));
        
        long workflowIdInResponse = 5;
        AttributeValue workflowIdInResponseAttributeValue = new AttributeValue();
        workflowIdInResponseAttributeValue.setN(String.valueOf(workflowIdInResponse));
        updatedValues.put(DDBBasedWorkflowIdGenerator.WORKFLOW_ID_KEY, workflowIdInResponseAttributeValue);
        result.setAttributes(updatedValues);
        when(generator.forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenReturn(result);
        
        when(generator.generateWorkflowId(anyString(), any(TSDMetrics.class))).thenCallRealMethod();
        
        Throwable caughtException = null;
        long newWorkflowId = workflowIdInResponse;
        try {
            newWorkflowId = generator.generateWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNull(caughtException);
        verify(generator, times(1)).forceUpdateWorkflowIdInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
        assertEquals(newWorkflowId, workflowIdInResponse);
    }
    
    /**
     * Test the normal case where the client calls to confirm acquiring workflowId and the lock in DDB is released cleanly.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testConfirmAcquiringWorkflowIdHappyCase() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        
        Mockito.doCallRealMethod().when(generator).confirmAcquiringWorkflowId(anyString(), anyLong(), any(TSDMetrics.class));
        generator.confirmAcquiringWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, 1, tsdMetrics);
        verify(generator, times(1)).updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where the client calls to confirm acquiring workflowId for a template which doesn't exist.
     * We expect an exception to be thrown in this case.
     */
    //@Test
    public void testConfirmFailsOnBadTemplate() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        
        Mockito.doCallRealMethod().when(generator).confirmAcquiringWorkflowId(anyString(), anyLong(), any(TSDMetrics.class));
        
        Throwable caughtException = null;
        try {
            generator.confirmAcquiringWorkflowId("BadTemplate", 1, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    /**
     * Test the case when all the update requests to DDB fail. We expect an exception to be thrown back in this case.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testConfirmFailsWhenAllUpdatesFail() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
        
        Mockito.doCallRealMethod().when(generator).confirmAcquiringWorkflowId(anyString(), anyLong(), any(TSDMetrics.class));
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException());
        
        Throwable caughtException = null;
        try {
            generator.confirmAcquiringWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, 1, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        verify(generator, times(DDBBasedWorkflowIdGenerator.DDB_UPDATE_MAX_RETRIES)).updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where some of the update requests to DDB fail, but we recover after a few retries.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testConfirmSuccessfulyAfterFewTransientFailures() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
         
        Mockito.doCallRealMethod().when(generator).confirmAcquiringWorkflowId(anyString(), anyLong(), any(TSDMetrics.class));
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new RuntimeException())
                                                                                            .thenThrow(new RuntimeException())
                                                                                            .thenReturn(new UpdateItemResult());
        
        generator.confirmAcquiringWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, 1, tsdMetrics);
        verify(generator, times(3)).updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
    
    /**
     * Test the case where on update to release the lock on row in DDB, we notice that our lock has been revoked.
     * We expect to not retry in such cases and fail fast, throwing back an exception.
     */
    @SuppressWarnings("unchecked")
    //@Test
    public void testConfirmFailsFastOnConditionalCheckFailedException() {
        DDBBasedWorkflowIdGenerator generator = mock(DDBBasedWorkflowIdGenerator.class);
         
        Mockito.doCallRealMethod().when(generator).confirmAcquiringWorkflowId(anyString(), anyLong(), any(TSDMetrics.class));
        
        when(generator.updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class))).thenThrow(new ConditionalCheckFailedException("condition failed"))
                                                                                            .thenReturn(new UpdateItemResult());
        
        Throwable caughtException = null;
        try {
            generator.confirmAcquiringWorkflowId(MitigationTemplate.Router_RateLimit_Route53Customer, 1, tsdMetrics);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        verify(generator, times(1)).updateItemInDDB(anyMap(), anyMap(), anyMap(), any(TSDMetrics.class));
    }
}
