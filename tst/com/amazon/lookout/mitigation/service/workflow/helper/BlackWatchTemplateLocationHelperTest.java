package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;

public class BlackWatchTemplateLocationHelperTest {
    private final List<String> locations = Arrays.asList("G-IAD55", "G-IAD5");

    private final TSDMetrics tsdMetrics = mock(TSDMetrics.class);
    
    @Before
    public void setUpBeforeTest() {
        when(tsdMetrics.newSubMetrics(anyString())).thenReturn(tsdMetrics);
    }
    
    @Test
    public void testGetLocationsFromRequest() {
        RequestInfoHandler requestInfoHanlder = mock(RequestInfoHandler.class);
        BlackWatchTemplateLocationHelper helper = new BlackWatchTemplateLocationHelper(requestInfoHanlder);
        
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setLocations(locations);
        
        assertEquals(new HashSet<String>(locations), helper.getLocationsForDeployment(request, tsdMetrics));
    }
    
    @Test
    public void testGetLocationsFromHistory() {
        RequestInfoHandler requestInfoHanlder = mock(RequestInfoHandler.class);
        BlackWatchTemplateLocationHelper helper = new BlackWatchTemplateLocationHelper(requestInfoHanlder);
        
        String serviceName = "R53";
        String deviceName = DeviceName.BLACKWATCH_BORDER.name();
        String deviceScope = DeviceScope.GLOBAL.name();
        String mitigationName = "miti1";
        
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setServiceName(serviceName);
        request.setMitigationTemplate(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer);
        request.setMitigationName(mitigationName);
        
        MitigationRequestDescriptionWithLocations requestDesc = new MitigationRequestDescriptionWithLocations();
        requestDesc.setLocations(locations);
        Mockito.doReturn(Arrays.asList(requestDesc)).when(requestInfoHanlder).getMitigationHistoryForMitigation(eq(serviceName),
                eq(deviceName), eq(deviceScope), eq(mitigationName), eq(null), eq(Integer.valueOf(1)),
                isA(TSDMetrics.class));
        
        assertEquals(new HashSet<String>(locations), helper.getLocationsForDeployment(request, tsdMetrics));
    }

}
