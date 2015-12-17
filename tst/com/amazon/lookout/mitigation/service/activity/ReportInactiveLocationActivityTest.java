package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.OperationNotSupportedException400;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ReportInactiveLocationActivityTest {
    EdgeLocationsHelper mockEdgeLocationsHelper = mock(EdgeLocationsHelper.class);
    BlackWatchBorderLocationValidator mockBlackWatchBorderLocationValidator = 
            mock(BlackWatchBorderLocationValidator.class);
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    @Test
    public void testRequestInProdDomain() {
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("TST1", "TST2", "TST3"));
        ServiceLocationsHelper serviceLocationsHelper = new ServiceLocationsHelper(edgeLocationsHelper);
        RequestValidator requestValidator = new RequestValidator(serviceLocationsHelper,
                mockEdgeLocationsHelper, mockBlackWatchBorderLocationValidator);
        
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        String domain = "prod";
        
        ReportInactiveLocationActivity activity = new ReportInactiveLocationActivity(requestValidator, activeMitigationInfoHandler, domain);
        
        // Bad request with unknown location. 
        ReportInactiveLocationRequest badRequest = new ReportInactiveLocationRequest();
        badRequest.setDeviceName(DeviceName.POP_ROUTER.name());
        badRequest.setServiceName(ServiceName.Route53);
        badRequest.setLocation("TST1");
        
        Exception caughtException = null;
        try {
            activity.enact(badRequest);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof OperationNotSupportedException400);
    }
    
    @Test
    public void testInvalidRequest() {
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("TST1", "TST2", "TST3"));
        ServiceLocationsHelper serviceLocationsHelper = new ServiceLocationsHelper(edgeLocationsHelper);
        RequestValidator requestValidator = new RequestValidator(serviceLocationsHelper,
                mockEdgeLocationsHelper, mockBlackWatchBorderLocationValidator);
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        String domain = "tst";
        
        ReportInactiveLocationActivity activity = new ReportInactiveLocationActivity(requestValidator, activeMitigationInfoHandler, domain);
        
        // Bad request with unknown location. 
        ReportInactiveLocationRequest badRequest = new ReportInactiveLocationRequest();
        badRequest.setDeviceName(DeviceName.POP_ROUTER.name());
        badRequest.setServiceName(ServiceName.Route53);
        badRequest.setLocation("TST4");
        
        Exception caughtException = null;
        try {
            activity.enact(badRequest);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof BadRequest400);
        
        // Bad request with bad serviceName location. 
        badRequest.setDeviceName(DeviceName.POP_ROUTER.name());
        badRequest.setServiceName("Random");
        badRequest.setLocation("TST1");

        caughtException = null;
        try {
            activity.enact(badRequest);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof BadRequest400);
        
        // Bad request with bad deviceName location. 
        badRequest.setDeviceName("Random");
        badRequest.setServiceName(ServiceName.Route53);
        badRequest.setLocation("TST1");

        caughtException = null;
        try {
            activity.enact(badRequest);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        assertTrue(caughtException instanceof BadRequest400);
    }
    
    @Test
    public void testNoActiveMitigationsForLocation() {
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("TST1", "TST2", "TST3"));
        ServiceLocationsHelper serviceLocationsHelper = new ServiceLocationsHelper(edgeLocationsHelper);
        RequestValidator requestValidator = new RequestValidator(serviceLocationsHelper,
                mockEdgeLocationsHelper, mockBlackWatchBorderLocationValidator);
        
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        String domain = "tst";
        
        ReportInactiveLocationActivity activity = new ReportInactiveLocationActivity(requestValidator, activeMitigationInfoHandler, domain);
        
        // Bad request with unknown location. 
        ReportInactiveLocationRequest badRequest = new ReportInactiveLocationRequest();
        badRequest.setDeviceName(DeviceName.POP_ROUTER.name());
        badRequest.setServiceName(ServiceName.Route53);
        badRequest.setLocation("TST1");
        
        when(activeMitigationInfoHandler.getActiveMitigationsForService(anyString(), anyString(), anyList(), any(TSDMetrics.class))).thenReturn(new ArrayList<ActiveMitigationDetails>());
        ReportInactiveLocationResponse response = activity.enact(badRequest);
        assertNotNull(response);
        assertEquals(response.getServiceName(), ServiceName.Route53);
        assertEquals(response.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(response.getLocation(), "TST1");
        assertTrue(response.getMitigationsMarkedAsDefunct().isEmpty());
    }
    
    @Test
    public void testMitigationsMarkedAsDefunct() {
        EdgeLocationsHelper edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        when(edgeLocationsHelper.getAllClassicPOPs()).thenReturn(Sets.newHashSet("TST1", "TST2", "TST3"));
        ServiceLocationsHelper serviceLocationsHelper = new ServiceLocationsHelper(edgeLocationsHelper);
        RequestValidator requestValidator = new RequestValidator(serviceLocationsHelper,
                mockEdgeLocationsHelper, mockBlackWatchBorderLocationValidator);
        
        ActiveMitigationInfoHandler activeMitigationInfoHandler = mock(ActiveMitigationInfoHandler.class);
        String domain = "tst";
        
        ReportInactiveLocationActivity activity = new ReportInactiveLocationActivity(requestValidator, activeMitigationInfoHandler, domain);
        
        // Bad request with unknown location. 
        ReportInactiveLocationRequest badRequest = new ReportInactiveLocationRequest();
        badRequest.setDeviceName(DeviceName.POP_ROUTER.name());
        badRequest.setServiceName(ServiceName.Route53);
        badRequest.setLocation("TST1");
        
        List<ActiveMitigationDetails> allActiveMitigations = new ArrayList<>();
        ActiveMitigationDetails details = new ActiveMitigationDetails("Mit1", 1, "TST1", DeviceName.POP_ROUTER.name(), 2, new DateTime().getMillis());
        allActiveMitigations.add(details);
        
        details = new ActiveMitigationDetails("Mit2", 3, "TST1", DeviceName.POP_ROUTER.name(), 1, new DateTime().getMillis());
        allActiveMitigations.add(details);
        
        details = new ActiveMitigationDetails("Mit5", 1, "TST1", DeviceName.POP_ROUTER.name(), 42, new DateTime().getMillis());
        allActiveMitigations.add(details);
        
        when(activeMitigationInfoHandler.getActiveMitigationsForService(anyString(), anyString(), anyList(), any(TSDMetrics.class))).thenReturn(allActiveMitigations);
        ReportInactiveLocationResponse response = activity.enact(badRequest);
        assertNotNull(response);
        assertEquals(response.getServiceName(), ServiceName.Route53);
        assertEquals(response.getDeviceName(), DeviceName.POP_ROUTER.name());
        assertEquals(response.getLocation(), "TST1");
        assertEquals(response.getMitigationsMarkedAsDefunct().size(), 3);
        assertEquals(response.getMitigationsMarkedAsDefunct(), Lists.newArrayList("Mit1", "Mit2", "Mit5"));
        verify(activeMitigationInfoHandler, times(3)).markMitigationAsDefunct(anyString(), anyString(), anyString(), anyString(), anyLong(), anyLong(), any(TSDMetrics.class));
    }
}
