package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;

public class ListBlackWatchMitigationsActivityTest extends ActivityTestHelper {
    private ListBlackWatchMitigationsRequest request;
    
    private ListBlackWatchMitigationsActivity listBlackWatchMitigationsActivity;
    
    @Before
    public void setup() {
        listBlackWatchMitigationsActivity = 
                spy(new ListBlackWatchMitigationsActivity(requestValidator, blackwatchMitigationInfoHandler));        
        request = new ListBlackWatchMitigationsRequest();
        request.setMitigationId("testMitigationId");
        request.setResourceId("testResourceId");
        request.setOwnerARN("testOwnerARN");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());
    }

    /**
     * Test list blackwatch mitigation activity works
     */
    @Test
    public void testListBlackWatchMitigationsActivity() {
        Mockito.doReturn(requestId).when(listBlackWatchMitigationsActivity).getRequestId();
        
        List<BlackWatchMitigationDefinition> listOfBlackWatchMitigationDefinition = new ArrayList<>();
               
        Mockito.doReturn(listOfBlackWatchMitigationDefinition).when(blackwatchMitigationInfoHandler).getBlackWatchMitigations(anyString(), anyString(), anyString(), anyString(), anyLong(), isA(TSDMetrics.class));
        
        ListBlackWatchMitigationsResponse response = listBlackWatchMitigationsActivity.enact(request);
        assertEquals(listOfBlackWatchMitigationDefinition, response.getMitigationList());
        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Test empty blackwatch mitigation list
     */
    @Test
    public void testListBlackWatchMitigationsActivity_EmptyList() {
        Mockito.doReturn(requestId).when(listBlackWatchMitigationsActivity).getRequestId();

        List<BlackWatchMitigationDefinition> listOfBlackWatchMitigationDefinition = new ArrayList<>();
        Mockito.doReturn(listOfBlackWatchMitigationDefinition).when(blackwatchMitigationInfoHandler).getBlackWatchMitigations(anyString(), anyString(), anyString(), anyString(), anyLong(), isA(TSDMetrics.class));
        ListBlackWatchMitigationsResponse response = listBlackWatchMitigationsActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
        assertTrue(response.getMitigationList().isEmpty());
    }
}