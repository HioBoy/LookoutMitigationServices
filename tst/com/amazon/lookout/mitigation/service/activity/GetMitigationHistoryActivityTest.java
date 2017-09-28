package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocationAndStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.test.common.util.TestUtils;

import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;
import com.amazon.lookout.mitigation.datastore.ArchivedRequestsDAO;
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

public class GetMitigationHistoryActivityTest extends ActivityTestHelper {

    private GetMitigationHistoryRequest request;
    
    @Before
    public void setup() {
        request = new GetMitigationHistoryRequest();
        request.setDeviceName(deviceName);
        request.setMitigationName(mitigationName);
        request.setServiceName(serviceName);
        request.setExclusiveStartVersion(exclusiveStartVersion);
        request.setMaxNumberOfHistoryEntriesToFetch(maxNumberOfHistoryEntriesToFetch);
    }
    
    private static final MitigationRequestDescription mitigationRequestDescription1 = new MitigationRequestDescription();
    private static final long jobId1 = 10000001l;
    private static final Integer mitigationVersion1 = 9;
    static {
        mitigationRequestDescription1.setJobId(jobId1);
        mitigationRequestDescription1.setMitigationVersion(mitigationVersion1);
    }
    private static final MitigationRequestDescription mitigationRequestDescription2 = new MitigationRequestDescription();
    private static final long jobId2 = 10000002l;
    private static final Integer mitigationVersion2 = 10;
    static {
        mitigationRequestDescription2.setJobId(jobId2);
        mitigationRequestDescription2.setMitigationVersion(mitigationVersion2);
    }
    private static final List<MitigationRequestDescriptionWithLocations> listOfMitigationRequestDescription;
    static {
        listOfMitigationRequestDescription = new ArrayList<>();
        MitigationRequestDescriptionWithLocations descWithLocation = new MitigationRequestDescriptionWithLocations();
        descWithLocation.setMitigationRequestDescription(mitigationRequestDescription1);
        descWithLocation.setLocations(locations);
        listOfMitigationRequestDescription.add(descWithLocation);
        descWithLocation = new MitigationRequestDescriptionWithLocations();
        descWithLocation.setMitigationRequestDescription(mitigationRequestDescription2);
        descWithLocation.setLocations(locations);
        listOfMitigationRequestDescription.add(descWithLocation);
    }
    
    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging();
    }

    /**
     * Test 2 history entries are retrieved
     */
    @Test
    public void test2HistoryFound() {
        GetMitigationHistoryActivity getMitigationHistoryActivity = 
                spy(new GetMitigationHistoryActivity(requestValidator, requestInfoHandler, mitigationInstanceInfoHandler,
                            mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class), mock(SwitcherooDAO.class)));

        Mockito.doNothing().when(requestValidator).validateGetMitigationHistoryRequest(request);
        List<MitigationRequestDescription> descs = new ArrayList<>();
        descs.add(new MitigationRequestDescription());
        Mockito.doReturn(descs).when(mitigationInstanceInfoHandler).getMitigationInstanceStatus(eq(deviceName), eq(jobId1), isA(TSDMetrics.class));
        Mockito.doReturn(descs).when(mitigationInstanceInfoHandler).getMitigationInstanceStatus(eq(deviceName), eq(jobId2), isA(TSDMetrics.class));
        Mockito.doReturn(requestId).when(getMitigationHistoryActivity).getRequestId();
        Mockito.doReturn(listOfMitigationRequestDescription).when(requestInfoHandler)
                .getMitigationHistoryForMitigation(eq(serviceName), eq(deviceName), eq(mitigationName), 
                        eq(exclusiveStartVersion), eq(maxNumberOfHistoryEntriesToFetch), isA(TSDMetrics.class));
        GetMitigationHistoryResponse response = getMitigationHistoryActivity.enact(request);
        assertEquals(deviceName, response.getDeviceName());
        assertEquals(mitigationVersion2, response.getExclusiveStartVersion());
        assertEquals(mitigationName, response.getMitigationName());
        assertEquals(requestId, response.getRequestId());
        assertEquals(serviceName, response.getServiceName());
        assertEquals(2, response.getListOfMitigationRequestDescriptionsWithLocationAndStatus().size());
        for (MitigationRequestDescriptionWithLocationAndStatus desc :
                response.getListOfMitigationRequestDescriptionsWithLocationAndStatus()) {
            assertEquals(locations, desc.getMitigationRequestDescriptionWithLocations().getLocations());
        }
    }
    
    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        GetMitigationHistoryActivity getMitigationHistoryActivity = 
                spy(new GetMitigationHistoryActivity(requestValidator, requestInfoHandler, mitigationInstanceInfoHandler,
                            mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class), mock(SwitcherooDAO.class)));

        Mockito.doThrow(new IllegalArgumentException()).when(requestValidator).validateGetMitigationHistoryRequest(request);
        getMitigationHistoryActivity.enact(request);
    }
    
    /**
     * Test empty list of mitigation history result
     */
    @Test
    public void testEmptyResult() {
        GetMitigationHistoryActivity getMitigationHistoryActivity = 
                spy(new GetMitigationHistoryActivity(requestValidator, requestInfoHandler, mitigationInstanceInfoHandler,
                            mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class), mock(SwitcherooDAO.class)));
        Mockito.doReturn(new ArrayList<MitigationRequestDescriptionWithLocations>()).when(requestInfoHandler)
                .getMitigationHistoryForMitigation(eq(serviceName), eq(deviceName), eq(mitigationName), 
                eq(exclusiveStartVersion), eq(maxNumberOfHistoryEntriesToFetch), isA(TSDMetrics.class));
        GetMitigationHistoryResponse response = getMitigationHistoryActivity.enact(request);
        assertTrue(response.getListOfMitigationRequestDescriptionsWithLocationAndStatus().isEmpty());
    }
    
    /**
     * Test history size larger than max
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidHistoryEntrySize() {
        GetMitigationHistoryActivity getMitigationHistoryActivity = 
                spy(new GetMitigationHistoryActivity(new RequestValidator(
                        mock(EdgeLocationsHelper.class), mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class), "/random/path/location/json"), requestInfoHandler, mitigationInstanceInfoHandler,
                            mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class), mock(SwitcherooDAO.class))); 
        request.setMaxNumberOfHistoryEntriesToFetch(10000);
        getMitigationHistoryActivity.enact(request); 
    }
    
    /**
     * Test negative history size
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidHistoryEntrySize2() {
        GetMitigationHistoryActivity getMitigationHistoryActivity = 
                spy(new GetMitigationHistoryActivity(new RequestValidator(
                        mock(EdgeLocationsHelper.class), mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class), "/random/path/location/json"), requestInfoHandler, mitigationInstanceInfoHandler,
                            mock(CurrentRequestsDAO.class), mock(ArchivedRequestsDAO.class), mock(SwitcherooDAO.class))); 
        request.setMaxNumberOfHistoryEntriesToFetch(-1);
        getMitigationHistoryActivity.enact(request);  
    }
}
