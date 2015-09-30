package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.model.RequestType;

public class ListActiveMitigationsForServiceActivityTest {

    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    /**
     * Test the case where none of the ongoing requests are part of the active ones.
     */
    @Test
    public void testMergeOngoingCreateRequests() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequests(anyList(), anyList());
        
        List<MitigationRequestDescriptionWithStatuses> activeMitigations = new ArrayList<>();
        
        List<MitigationRequestDescriptionWithStatuses> ongoingRequests = new ArrayList<>();
        MitigationRequestDescriptionWithStatuses ongoingRequestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DEPLOY_FAILED);
        instancesStatus.put("TST2", status);
        
        MitigationRequestDescription desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setJobId(1);
        desc.setRequestType(RequestType.CreateRequest.name());
        ongoingRequestWithStatuses.setMitigationRequestDescription(desc);
        ongoingRequestWithStatuses.setInstancesStatusMap(instancesStatus);
        ongoingRequests.add(ongoingRequestWithStatuses);
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> response = activity.mergeOngoingRequests(ongoingRequests, activeMitigations);
        assertEquals(response.size(), 1);
        
        List<MitigationRequestDescriptionWithStatuses> responseMitigations = response.get(response.keySet().iterator().next());
        assertEquals(responseMitigations.size(), 1);
        
        MitigationRequestDescriptionWithStatuses mitigationDescResponse = responseMitigations.get(0);
        assertNotNull(mitigationDescResponse);
        assertEquals(mitigationDescResponse.getInstancesStatusMap().size(), 1);
        assertTrue(mitigationDescResponse.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(mitigationDescResponse.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertEquals(mitigationDescResponse.getMitigationRequestDescription(), desc);
    }
    
    /**
     * Test the case where some of the ongoing delete requests are part of the active ones, so we need to merge them.
     */
    @Test
    public void testMergeOngoingDeleteRequestsWithSomeActiveOnes() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequests(anyList(), anyList());
        
        List<MitigationRequestDescriptionWithStatuses> ongoingRequests = new ArrayList<>();
        
        // Create an ongoing request instance.
        MitigationRequestDescriptionWithStatuses ongoingRequestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DELETE_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DELETING);
        instancesStatus.put("TST2", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST3");
        status.setMitigationStatus(MitigationStatus.DELETE_FAILED);
        instancesStatus.put("TST3", status);
        
        MitigationRequestDescription commonMitigationDesc = new MitigationRequestDescription();
        commonMitigationDesc.setMitigationName("CommonMitigation");
        commonMitigationDesc.setDeviceName("TestDevice");
        commonMitigationDesc.setJobId(5);
        commonMitigationDesc.setRequestType(RequestType.DeleteRequest.name());
        ongoingRequestWithStatuses.setMitigationRequestDescription(commonMitigationDesc);
        ongoingRequestWithStatuses.setInstancesStatusMap(instancesStatus);
        ongoingRequests.add(ongoingRequestWithStatuses);
        
        // Create another ongoing delete request which doesn't have a corresponding active mitigation.
        ongoingRequestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DELETE_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        MitigationRequestDescription deleteMitigationDesc = new MitigationRequestDescription();
        deleteMitigationDesc.setMitigationName("DeleteMitigation1");
        deleteMitigationDesc.setDeviceName("TestDevice");
        deleteMitigationDesc.setJobId(5);
        deleteMitigationDesc.setRequestType(RequestType.DeleteRequest.name());
        ongoingRequestWithStatuses.setMitigationRequestDescription(deleteMitigationDesc);
        ongoingRequestWithStatuses.setInstancesStatusMap(instancesStatus);
        ongoingRequests.add(ongoingRequestWithStatuses);
        
        List<MitigationRequestDescriptionWithStatuses> activeMitigations = new ArrayList<>();
        // Create the corresponding active mitigation instance.
        MitigationRequestDescriptionWithStatuses activeMitigationWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST3");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST3", status);
        
        commonMitigationDesc = new MitigationRequestDescription();
        commonMitigationDesc.setMitigationName("CommonMitigation");
        commonMitigationDesc.setDeviceName("TestDevice");
        commonMitigationDesc.setJobId(1);
        commonMitigationDesc.setRequestType(RequestType.CreateRequest.name());
        activeMitigationWithStatuses.setMitigationRequestDescription(commonMitigationDesc);
        activeMitigationWithStatuses.setInstancesStatusMap(instancesStatus);
        activeMitigations.add(activeMitigationWithStatuses);
        
        // Create another instance of an active mitigation which isn't being acted upon.
        activeMitigationWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST2", status);
        
        MitigationRequestDescription activeMitigationDesc = new MitigationRequestDescription();
        activeMitigationDesc.setMitigationName("ActiveMitigation2");
        activeMitigationDesc.setDeviceName("TestDevice");
        activeMitigationDesc.setJobId(1);
        activeMitigationDesc.setRequestType(RequestType.CreateRequest.name());
        activeMitigationWithStatuses.setMitigationRequestDescription(activeMitigationDesc);
        activeMitigationWithStatuses.setInstancesStatusMap(instancesStatus);
        activeMitigations.add(activeMitigationWithStatuses);
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigationsMap = activity.mergeOngoingRequests(ongoingRequests, activeMitigations);
        assertEquals(mergedMitigationsMap.size(), 3);
        
        MitigationRequestDescriptionWithStatuses commonMitigationResponse = null;
        MitigationRequestDescriptionWithStatuses activeMitigation2Response = null;
        MitigationRequestDescriptionWithStatuses deleteMitigationResponse = null;
        
        for (List<MitigationRequestDescriptionWithStatuses> mitigations : mergedMitigationsMap.values()) {
            assertEquals(mitigations.size(), 1);
            for (MitigationRequestDescriptionWithStatuses mitigation : mitigations) {
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("CommonMitigation")) {
                    commonMitigationResponse = mitigation;
                }
                
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("ActiveMitigation2")) {
                    activeMitigation2Response = mitigation;
                }
                
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("DeleteMitigation1")) {
                    deleteMitigationResponse = mitigation;
                }
            }
        }
        
        assertNotNull(commonMitigationResponse);
        assertNotNull(activeMitigation2Response);
        assertNotNull(deleteMitigationResponse);
        
        assertEquals(commonMitigationResponse.getInstancesStatusMap().size(), 3);
        assertTrue(commonMitigationResponse.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(commonMitigationResponse.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DELETE_SUCCEEDED);
        assertTrue(commonMitigationResponse.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(commonMitigationResponse.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DELETING);
        assertTrue(commonMitigationResponse.getInstancesStatusMap().containsKey("TST3"));
        assertEquals(commonMitigationResponse.getInstancesStatusMap().get("TST3").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        commonMitigationDesc.setUpdateJobId(5);
        assertEquals(commonMitigationResponse.getMitigationRequestDescription(), commonMitigationDesc);
        
        assertEquals(activeMitigation2Response.getInstancesStatusMap().size(), 2);
        assertTrue(activeMitigation2Response.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(activeMitigation2Response.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertTrue(activeMitigation2Response.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(activeMitigation2Response.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertEquals(activeMitigation2Response.getMitigationRequestDescription(), activeMitigationDesc);
        
        assertEquals(deleteMitigationResponse.getInstancesStatusMap().size(), 1);
        assertTrue(deleteMitigationResponse.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(deleteMitigationResponse.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DELETE_SUCCEEDED);
        assertEquals(deleteMitigationResponse.getMitigationRequestDescription(), deleteMitigationDesc);
    }
    
    /**
     * Test the case where some of the ongoing edit requests are part of the active ones, so we need to merge them.
     */
    @Test
    public void testMergeOngoingEditRequestsWithSomeActiveOnes() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequests(anyList(), anyList());
        
        List<MitigationRequestDescriptionWithStatuses> ongoingRequests = new ArrayList<>();
        
        // Create an ongoing request instance.
        MitigationRequestDescriptionWithStatuses ongoingRequestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.EDIT_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.EDITING);
        instancesStatus.put("TST2", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST3");
        status.setMitigationStatus(MitigationStatus.EDIT_FAILED);
        instancesStatus.put("TST3", status);
        
        MitigationRequestDescription editCommonMitigationDesc = new MitigationRequestDescription();
        editCommonMitigationDesc.setMitigationName("CommonMitigation");
        editCommonMitigationDesc.setDeviceName("TestDevice");
        editCommonMitigationDesc.setJobId(5);
        editCommonMitigationDesc.setRequestType(RequestType.EditRequest.name());
        ongoingRequestWithStatuses.setMitigationRequestDescription(editCommonMitigationDesc);
        ongoingRequestWithStatuses.setInstancesStatusMap(instancesStatus);
        ongoingRequests.add(ongoingRequestWithStatuses);
        
        // Create another ongoing delete request which doesn't have a corresponding active mitigation.
        ongoingRequestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.EDIT_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        MitigationRequestDescription editMitigation1Desc = new MitigationRequestDescription();
        editMitigation1Desc.setMitigationName("EditMitigation1");
        editMitigation1Desc.setDeviceName("TestDevice");
        editMitigation1Desc.setJobId(3);
        editMitigation1Desc.setRequestType(RequestType.EditRequest.name());
        ongoingRequestWithStatuses.setMitigationRequestDescription(editMitigation1Desc);
        ongoingRequestWithStatuses.setInstancesStatusMap(instancesStatus);
        ongoingRequests.add(ongoingRequestWithStatuses);
        
        List<MitigationRequestDescriptionWithStatuses> activeMitigations = new ArrayList<>();
        // Create the corresponding active mitigation instance.
        MitigationRequestDescriptionWithStatuses activeMitigationWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST3");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST3", status);
        
        MitigationRequestDescription commonMitigationDesc = new MitigationRequestDescription();
        commonMitigationDesc.setMitigationName("CommonMitigation");
        commonMitigationDesc.setDeviceName("TestDevice");
        commonMitigationDesc.setJobId(1);
        commonMitigationDesc.setRequestType(RequestType.CreateRequest.name());
        activeMitigationWithStatuses.setMitigationRequestDescription(commonMitigationDesc);
        activeMitigationWithStatuses.setInstancesStatusMap(instancesStatus);
        activeMitigations.add(activeMitigationWithStatuses);
        
        // Create another instance of an active mitigation which isn't being acted upon.
        activeMitigationWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST2", status);
        
        MitigationRequestDescription activeMitigationDesc = new MitigationRequestDescription();
        activeMitigationDesc.setMitigationName("ActiveMitigation2");
        activeMitigationDesc.setDeviceName("TestDevice");
        activeMitigationDesc.setJobId(2);
        activeMitigationDesc.setRequestType(RequestType.CreateRequest.name());
        activeMitigationWithStatuses.setMitigationRequestDescription(activeMitigationDesc);
        activeMitigationWithStatuses.setInstancesStatusMap(instancesStatus);
        activeMitigations.add(activeMitigationWithStatuses);
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigationsMap = activity.mergeOngoingRequests(ongoingRequests, activeMitigations);
        assertEquals(mergedMitigationsMap.size(), 3);
        
        MitigationRequestDescriptionWithStatuses commonMitigationResponse = null;
        MitigationRequestDescriptionWithStatuses editCommonMitigationResponse = null;
        MitigationRequestDescriptionWithStatuses activeMitigation2Response = null;
        MitigationRequestDescriptionWithStatuses editMitigation1Response = null;
        
        for (List<MitigationRequestDescriptionWithStatuses> mitigations : mergedMitigationsMap.values()) {
            for (MitigationRequestDescriptionWithStatuses mitigation : mitigations) {
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("CommonMitigation")) {
                    assertEquals(mitigations.size(), 2);
                    if (mitigation.getMitigationRequestDescription().getJobId() == 1) {
                        commonMitigationResponse = mitigation;
                    }
                    
                    if (mitigation.getMitigationRequestDescription().getJobId() == 5) {
                        editCommonMitigationResponse = mitigation;
                    }
                }
                
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("ActiveMitigation2")) {
                    assertEquals(mitigations.size(), 1);
                    activeMitigation2Response = mitigation;
                }
                
                if (mitigation.getMitigationRequestDescription().getMitigationName().equals("EditMitigation1")) {
                    assertEquals(mitigations.size(), 1);
                    editMitigation1Response = mitigation;
                }
            }
        }
        
        assertNotNull(commonMitigationResponse);
        assertNotNull(editCommonMitigationResponse);
        assertNotNull(activeMitigation2Response);
        assertNotNull(editMitigation1Response);
        
        assertEquals(commonMitigationResponse.getInstancesStatusMap().size(), 1);
        assertTrue(commonMitigationResponse.getInstancesStatusMap().containsKey("TST3"));
        assertEquals(commonMitigationResponse.getInstancesStatusMap().get("TST3").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        
        commonMitigationDesc.setUpdateJobId(5);
        assertEquals(commonMitigationResponse.getMitigationRequestDescription(), commonMitigationDesc);
        
        assertEquals(editCommonMitigationResponse.getInstancesStatusMap().size(), 2);
        assertTrue(editCommonMitigationResponse.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(editCommonMitigationResponse.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.EDIT_SUCCEEDED);
        assertTrue(editCommonMitigationResponse.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(editCommonMitigationResponse.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.EDITING);
        assertEquals(editCommonMitigationResponse.getMitigationRequestDescription(), editCommonMitigationDesc);
        
        assertEquals(activeMitigation2Response.getInstancesStatusMap().size(), 2);
        assertTrue(activeMitigation2Response.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(activeMitigation2Response.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertTrue(activeMitigation2Response.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(activeMitigation2Response.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertEquals(activeMitigation2Response.getMitigationRequestDescription(), activeMitigationDesc);
        
        assertEquals(editMitigation1Response.getInstancesStatusMap().size(), 1);
        assertTrue(editMitigation1Response.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(editMitigation1Response.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.EDIT_SUCCEEDED);
        assertEquals(editMitigation1Response.getMitigationRequestDescription(), editMitigation1Desc);
    }
    
    /**
     * Test the case where some of the ongoing requests are part of the active ones, so we need to merge the missing locations.
     */
    @Test
    public void testMergeLocationsOngoingRequestsWithActiveOnes() {
        ListActiveMitigationsForServiceActivity activity = mock(ListActiveMitigationsForServiceActivity.class);
        doCallRealMethod().when(activity).mergeOngoingRequests(anyList(), anyList());
        
        List<MitigationRequestDescriptionWithStatuses> activeMitigations = new ArrayList<>();
        MitigationRequestDescriptionWithStatuses requestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        MitigationRequestDescription desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setJobId(1);
        desc.setRequestType(RequestType.CreateRequest.name());
        
        Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
        MitigationInstanceStatus status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        requestWithStatuses.setMitigationRequestDescription(desc);
        requestWithStatuses.setInstancesStatusMap(instancesStatus);
        activeMitigations.add(requestWithStatuses);
        
        List<MitigationRequestDescriptionWithStatuses> ongoingRequests = new ArrayList<>();
        requestWithStatuses = new MitigationRequestDescriptionWithStatuses();
        
        instancesStatus = new HashMap<>();
        status = new MitigationInstanceStatus();
        status.setLocation("TST1");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST1", status);
        
        status = new MitigationInstanceStatus();
        status.setLocation("TST2");
        status.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
        instancesStatus.put("TST2", status);
        
        requestWithStatuses.setInstancesStatusMap(instancesStatus);
        
        desc = new MitigationRequestDescription();
        desc.setDeviceName("TestDevice");
        desc.setRequestType(RequestType.CreateRequest.name());
        desc.setJobId(1);
        requestWithStatuses.setMitigationRequestDescription(desc);
        ongoingRequests.add(requestWithStatuses);
        
        Map<String, List<MitigationRequestDescriptionWithStatuses>> response = activity.mergeOngoingRequests(ongoingRequests, activeMitigations);
        assertEquals(response.size(), 1);
        
        List<MitigationRequestDescriptionWithStatuses> responseMitigations = response.get(response.keySet().iterator().next());
        assertEquals(responseMitigations.size(), 1);
        
        MitigationRequestDescriptionWithStatuses mitigationDescResponse = responseMitigations.get(0);
        
        assertNotNull(mitigationDescResponse);
        assertEquals(mitigationDescResponse.getInstancesStatusMap().size(), 2);
        assertTrue(mitigationDescResponse.getInstancesStatusMap().containsKey("TST1"));
        assertEquals(mitigationDescResponse.getInstancesStatusMap().get("TST1").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertTrue(mitigationDescResponse.getInstancesStatusMap().containsKey("TST2"));
        assertEquals(mitigationDescResponse.getInstancesStatusMap().get("TST2").getMitigationStatus(), MitigationStatus.DEPLOY_SUCCEEDED);
        assertEquals(mitigationDescResponse.getMitigationRequestDescription(), desc);
    }
}
