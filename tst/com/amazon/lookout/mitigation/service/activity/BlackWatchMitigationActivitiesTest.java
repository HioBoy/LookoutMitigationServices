package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig.GlobalTrafficShaper;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig.MitigationConfig;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsResponse;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;

public class BlackWatchMitigationActivitiesTest extends DDBBasedActivityTestHelper {
    
    private ApplyBlackWatchMitigationActivity applyBlackWatchMitigationActivity;
    private UpdateBlackWatchMitigationActivity updateBlackWatchMitigationActivity;
    private ListBlackWatchMitigationsActivity listBlackWatchMitigationsActivity;
    
    @Before
    public void createActivities() {
        applyBlackWatchMitigationActivity = setupActivity(
                new ApplyBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));
        updateBlackWatchMitigationActivity = setupActivity(
                new UpdateBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));
        listBlackWatchMitigationsActivity = setupActivity(
                new ListBlackWatchMitigationsActivity(requestValidator, blackwatchMitigationInfoHandler));
    }
    
    private BlackWatchMitigationDefinition assertMitigationExists(String mitigationId) {
        ListBlackWatchMitigationsRequest request = new ListBlackWatchMitigationsRequest();
        request.setMitigationId(mitigationId);
        request.setMitigationActionMetadata(mitigationActionMetadata);
        
        ListBlackWatchMitigationsResponse response = listBlackWatchMitigationsActivity.enact(request);
        
        assertNotNull(response);
        assertNotNull(response.getMitigationList());
        assertEquals(1, response.getMitigationList().size());
        BlackWatchMitigationDefinition mitigation = response.getMitigationList().get(0);
        assertNotNull(mitigation);
        
        return mitigation;
    }
    
    @Test
    public void testApplyNewIPAddressMitigation() {
        ApplyBlackWatchMitigationRequest request1 = new ApplyBlackWatchMitigationRequest();
        request1.setResourceType("IPAddress");
        request1.setResourceId("1.2.3.4");
        request1.setMitigationActionMetadata(mitigationActionMetadata);
        request1.setGlobalBPS(1500000L);
        request1.setGlobalPPS(1000L);
        
        ApplyBlackWatchMitigationResponse response1 = applyBlackWatchMitigationActivity.enact(request1);
        
        assertNotNull(response1);
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        assertTrue(response1.isNewMitigationCreated());
        assertEquals(requestId, response1.getRequestId());
        
        BlackWatchMitigationDefinition mitigation1 = assertMitigationExists(mitigationId);
        
        assertEquals(mitigationId, mitigation1.getMitigationId());
        assertEquals("IPAddress", mitigation1.getResourceType());
        assertEquals("1.2.3.4/32", mitigation1.getResourceId());
        assertEquals(mitigationActionMetadata, mitigation1.getLatestMitigationActionMetadata());
        assertEquals(Long.valueOf(1500000L), mitigation1.getGlobalBPS());
        assertEquals(Long.valueOf(1000L), mitigation1.getGlobalPPS());
        assertEquals("Active", mitigation1.getState());
    }

    @Test
    public void testApplyExistingIPAddressMitigation() {
        ApplyBlackWatchMitigationRequest request1 = new ApplyBlackWatchMitigationRequest();
        request1.setResourceType("IPAddress");
        request1.setResourceId("1.2.3.4");
        request1.setMitigationActionMetadata(mitigationActionMetadata);
        request1.setGlobalBPS(1500000L);
        request1.setGlobalPPS(1000L);
        
        ApplyBlackWatchMitigationResponse response1 = applyBlackWatchMitigationActivity.enact(request1);
        
        assertNotNull(response1);
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        assertTrue(response1.isNewMitigationCreated());
        assertEquals(requestId, response1.getRequestId());
        
        BlackWatchMitigationDefinition mitigation1 = assertMitigationExists(mitigationId);
        
        assertEquals(mitigationId, mitigation1.getMitigationId());
        assertEquals("IPAddress", mitigation1.getResourceType());
        assertEquals("1.2.3.4/32", mitigation1.getResourceId());
        assertEquals(mitigationActionMetadata, mitigation1.getLatestMitigationActionMetadata());
        assertEquals(Long.valueOf(1500000L), mitigation1.getGlobalBPS());
        assertEquals(Long.valueOf(1000L), mitigation1.getGlobalPPS());
        assertEquals("Active", mitigation1.getState());
        
        // second request should return existing mitigation
        ApplyBlackWatchMitigationRequest request2 = new ApplyBlackWatchMitigationRequest();
        request2.setResourceType("IPAddress");
        request2.setResourceId("1.2.3.4");
        request2.setMitigationActionMetadata(mitigationActionMetadata);
        request2.setGlobalBPS(3000000L);
        request2.setGlobalPPS(2000L);
        
        ApplyBlackWatchMitigationResponse response2 = applyBlackWatchMitigationActivity.enact(request2);
        
        assertNotNull(response2);
        assertEquals(response1.getMitigationId(), response2.getMitigationId());
        assertFalse(response2.isNewMitigationCreated());
        
        BlackWatchMitigationDefinition mitigation2 = assertMitigationExists(mitigationId);
        
        assertEquals(mitigationId, mitigation2.getMitigationId());
        assertEquals("IPAddress", mitigation2.getResourceType());
        assertEquals("1.2.3.4/32", mitigation2.getResourceId());
        assertEquals(mitigationActionMetadata, mitigation2.getLatestMitigationActionMetadata());
        assertEquals(Long.valueOf(3000000L), mitigation2.getGlobalBPS());
        assertEquals(Long.valueOf(2000L), mitigation2.getGlobalPPS());
        assertEquals("Active", mitigation2.getState());
    }

    @Test
    public void testApplyWithJSONUpdateWithoutJSON() {
        ApplyBlackWatchMitigationRequest request1 = new ApplyBlackWatchMitigationRequest();
        request1.setResourceType("IPAddress");
        request1.setResourceId("1.2.3.4");
        request1.setMitigationActionMetadata(mitigationActionMetadata);
        
        BlackWatchTargetConfig targetConfig1 = new BlackWatchTargetConfig();
        MitigationConfig mitigationConfig1 = new MitigationConfig();
        Map<String, GlobalTrafficShaper> globalTrafficShapers1 = new HashMap<>();
        GlobalTrafficShaper defaultTrafficShaper1 = new GlobalTrafficShaper();
        defaultTrafficShaper1.setGlobal_pps(1000L);
        globalTrafficShapers1.put("default", defaultTrafficShaper1);
        mitigationConfig1.setGlobal_traffic_shaper(globalTrafficShapers1);
        targetConfig1.setMitigation_config(mitigationConfig1);
        String mitigationSettingsJson1 = targetConfig1.getJsonString();
        request1.setMitigationSettingsJSON(mitigationSettingsJson1);
        
        ApplyBlackWatchMitigationResponse response1 = applyBlackWatchMitigationActivity.enact(request1);
        
        assertNotNull(response1);
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        assertTrue(response1.isNewMitigationCreated());
        
        BlackWatchMitigationDefinition mitigation1 = assertMitigationExists(mitigationId);
        
        assertEquals(mitigationSettingsJson1, mitigation1.getMitigationSettingsJSON());

        UpdateBlackWatchMitigationRequest request2 = new UpdateBlackWatchMitigationRequest();
        request2.setMitigationId(mitigationId);
        request2.setMitigationActionMetadata(mitigationActionMetadata);
        
        UpdateBlackWatchMitigationResponse response2 = updateBlackWatchMitigationActivity.enact(request2);
        
        assertNotNull(response2);
        assertEquals(mitigationId, response2.getMitigationId());
        assertEquals(userArn, response2.getPreviousOwnerARN());
        assertEquals(requestId, response2.getRequestId());
        
        BlackWatchMitigationDefinition mitigation2 = assertMitigationExists(mitigationId);
        
        assertEquals(mitigationSettingsJson1, mitigation2.getMitigationSettingsJSON());
    }
    
}
