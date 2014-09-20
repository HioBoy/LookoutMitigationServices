package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
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
    
    /**
     * Test to ensure we filter out the locations that have failed mitigations. If an request has all locations failed, ensure that we filter that entire request out.
     */
    @Test
    public void testFilterOutRequestsWithAllLocationsFailed() {
        RequestValidator validator = mock(RequestValidator.class);
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        RequestInfoHandler requestInfoHandler = mock(RequestInfoHandler.class);
        MitigationInstanceInfoHandler mitigationInstanceHandler = mock(MitigationInstanceInfoHandler.class);
        ListActiveMitigationsForServiceActivity activity = new ListActiveMitigationsForServiceActivity(validator, activeMitigationInfoHandler, requestInfoHandler, mitigationInstanceHandler);
        
        String deviceName = "TestDevice";
        long jobId1 = 1;
        long jobId2 = 2;
        
        TSDMetrics tsdMetrics = mock(TSDMetrics.class);
        
        List<MitigationRequestDescriptionWithLocations> listOfDescriptions = new ArrayList<>();
        MitigationRequestDescriptionWithLocations descWithLocations = new MitigationRequestDescriptionWithLocations();
        MitigationRequestDescription desc = new MitigationRequestDescription();
        desc.setJobId(jobId1);
        desc.setDeviceName(deviceName);
        descWithLocations.setMitigationRequestDescription(desc);
        listOfDescriptions.add(descWithLocations);
        
        descWithLocations = new MitigationRequestDescriptionWithLocations();
        desc = new MitigationRequestDescription();
        desc.setJobId(jobId2);
        desc.setDeviceName(deviceName);
        descWithLocations.setMitigationRequestDescription(desc);
        listOfDescriptions.add(descWithLocations);
        
        List<MitigationInstanceStatus> statuses1 = new ArrayList<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_FAILED);
        statuses1.add(status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        statuses1.add(status);
        when(mitigationInstanceHandler.getMitigationInstanceStatus(deviceName, jobId1, tsdMetrics)).thenReturn(statuses1);
        
        List<MitigationInstanceStatus> statuses2 = new ArrayList<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_FAILED);
        statuses2.add(status);
        when(mitigationInstanceHandler.getMitigationInstanceStatus(deviceName, jobId2, tsdMetrics)).thenReturn(statuses2);
        
        List<MitigationRequestDescriptionWithLocations> filteredList = activity.filterOutRequestsWithAllLocationsFailed(listOfDescriptions, tsdMetrics);
        assertEquals(filteredList.size(), 1);
        assertEquals(filteredList.get(0).getLocations().size(), 1);
        assertEquals(filteredList.get(0).getLocations().get(0), "TST2");
        assertEquals(filteredList.get(0).getMitigationRequestDescription().getDeviceName(), deviceName);
        assertEquals(filteredList.get(0).getMitigationRequestDescription().getJobId(), jobId1);
    }
}
