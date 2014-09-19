package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.google.common.collect.Lists;

public class ListActiveMitigationsForServiceActivityTest {

    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    /**
     * Test the case where none of the ongoing requests are part of the active ones.
     */
    @Test
    public void testMergeAllOngoingRequestsWithActiveOnes() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequestsToActiveOnes(anyList(), anyMap());
        
        Map<String, MitigationRequestDescriptionWithLocations> activeMitigations = new HashMap<>();
        
        List<MitigationRequestDescriptionWithLocations> ongoingRequests = new ArrayList<>();
        MitigationRequestDescriptionWithLocations requestWithLocations = new MitigationRequestDescriptionWithLocations();
        
        List<String> locations = Lists.newArrayList("TST1", "TST2");
        requestWithLocations.setLocations(locations);
        
        MitigationRequestDescription desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setJobId(1);
        requestWithLocations.setMitigationRequestDescription(desc);
        ongoingRequests.add(requestWithLocations);
        
        activity.mergeOngoingRequestsToActiveOnes(ongoingRequests, activeMitigations);
        assertEquals(activeMitigations.size(), 1);
        
        MitigationRequestDescriptionWithLocations requestDescWithLocations = activeMitigations.get(desc.getDeviceName() + ListActiveMitigationsForServiceActivity.KEY_SEPARATOR + desc.getJobId());
        assertNotNull(requestDescWithLocations);
        assertEquals(requestDescWithLocations.getLocations(), locations);
        assertEquals(requestDescWithLocations.getMitigationRequestDescription(), desc);
    }
    
    /**
     * Test the case where some of the ongoing requests are part of the active ones, so we need to merge the missing locations.
     */
    @Test
    public void testMergeLocationsOngoingRequestsWithActiveOnes() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequestsToActiveOnes(anyList(), anyMap());
        
        Map<String, MitigationRequestDescriptionWithLocations> activeMitigations = new HashMap<>();
        MitigationRequestDescriptionWithLocations requestWithLocations = new MitigationRequestDescriptionWithLocations();
        
        requestWithLocations.setLocations(Lists.newArrayList("TST1"));
        
        MitigationRequestDescription desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setJobId(1);
        requestWithLocations.setMitigationRequestDescription(desc);
        activeMitigations.put(desc.getDeviceName() + ListActiveMitigationsForServiceActivity.KEY_SEPARATOR + desc.getJobId(), requestWithLocations);
        
        List<MitigationRequestDescriptionWithLocations> ongoingRequests = new ArrayList<>();
        requestWithLocations = new MitigationRequestDescriptionWithLocations();
        
        List<String> locations = Lists.newArrayList("TST1", "TST2");
        requestWithLocations.setLocations(locations);
        
        desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setJobId(1);
        requestWithLocations.setMitigationRequestDescription(desc);
        ongoingRequests.add(requestWithLocations);
        
        activity.mergeOngoingRequestsToActiveOnes(ongoingRequests, activeMitigations);
        assertEquals(activeMitigations.size(), 1);
        
        MitigationRequestDescriptionWithLocations requestDescWithLocations = activeMitigations.get(desc.getDeviceName() + ListActiveMitigationsForServiceActivity.KEY_SEPARATOR + desc.getJobId());
        assertNotNull(requestDescWithLocations);
        assertEquals(requestDescWithLocations.getLocations(), locations);
        assertEquals(requestDescWithLocations.getMitigationRequestDescription(), desc);
    }
}
