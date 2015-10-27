package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryResponse;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.MissingLocationException400;

public class GetLocationDeploymentHistoryActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private static final Long exclusiveLastEvaluatedTimestamp = 10000001l;
    private static final GetLocationDeploymentHistoryRequest request = new GetLocationDeploymentHistoryRequest();
    static {
        request.setDeviceName(deviceName);
        request.setExclusiveLastEvaluatedTimestamp(exclusiveLastEvaluatedTimestamp);
        request.setLocation(location);
        request.setServiceName(serviceName);
        request.setMaxNumberOfHistoryEntriesToFetch(maxNumberOfHistoryEntriesToFetch);
    }
    
    /**
     * Test when no location history is found, MissingLocationException400 is thrown.
     */
    @Test(expected = MissingLocationException400.class)
    public void testNoLocationHistoryFound() {
        GetLocationDeploymentHistoryActivity getLocationDeploymentHistoryActivity = 
                new GetLocationDeploymentHistoryActivity(requestValidator, mitigationInstanceInfoHandler);
        
        Mockito.doNothing().when(requestValidator).validateGetLocationDeploymentHistoryRequest(request);
        
        Mockito.doThrow(new MissingLocationException400()).when(mitigationInstanceInfoHandler)
                .getLocationDeploymentInfoOnLocation(eq(deviceName), eq(serviceName), eq(location), eq(maxNumberOfHistoryEntriesToFetch),
                        eq(exclusiveLastEvaluatedTimestamp), isA(TSDMetrics.class));
        
        getLocationDeploymentHistoryActivity.enact(request);
    }

    /**
     * Test location history works correctly.
     */
    @Test
    public void test3LocationHistory() {
        GetLocationDeploymentHistoryActivity getLocationDeploymentHistoryActivity = 
                spy(new GetLocationDeploymentHistoryActivity(requestValidator, mitigationInstanceInfoHandler));
        
        Mockito.doNothing().when(requestValidator).validateGetLocationDeploymentHistoryRequest(request);
        Mockito.doReturn(requestId).when(getLocationDeploymentHistoryActivity).getRequestId();
        
        List<LocationDeploymentInfo> listOfLocationDeploymentInfo = new ArrayList<>();
        LocationDeploymentInfo locationDeploymentInfo = new LocationDeploymentInfo();
        DateTime now = DateTime.now();
        locationDeploymentInfo.setDeployDate(now.toString(MitigationInstancesModel.CREATE_DATE_FORMATTER));
        listOfLocationDeploymentInfo.add(locationDeploymentInfo);
        locationDeploymentInfo = new LocationDeploymentInfo();
        locationDeploymentInfo.setDeployDate(now.minusMinutes(1).toString(MitigationInstancesModel.CREATE_DATE_FORMATTER));
        listOfLocationDeploymentInfo.add(locationDeploymentInfo);
        locationDeploymentInfo = new LocationDeploymentInfo();
        locationDeploymentInfo.setDeployDate(now.minusMinutes(2).toString(MitigationInstancesModel.CREATE_DATE_FORMATTER));
        listOfLocationDeploymentInfo.add(locationDeploymentInfo);
        
        Mockito.doReturn(listOfLocationDeploymentInfo).when(mitigationInstanceInfoHandler)
                .getLocationDeploymentInfoOnLocation(eq(deviceName), eq(serviceName), eq(location),
                        eq(maxNumberOfHistoryEntriesToFetch), eq(exclusiveLastEvaluatedTimestamp), isA(TSDMetrics.class));
        
        GetLocationDeploymentHistoryResponse response = getLocationDeploymentHistoryActivity.enact(request);
        
        assertEquals(deviceName, response.getDeviceName());
        // ignore the milliseconds, as it get lost during conversion
        assertEquals(now.minusMinutes(2).getMillis() / 1000, response.getExclusiveLastEvaluatedTimestamp() / 1000);
        assertEquals(listOfLocationDeploymentInfo, response.getListOfLocationDeploymentInfo());
        assertEquals(location, response.getLocation());
        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        GetLocationDeploymentHistoryActivity getLocationDeploymentHistoryActivity = 
                new GetLocationDeploymentHistoryActivity(requestValidator, mitigationInstanceInfoHandler);

        Mockito.doThrow(new IllegalArgumentException()).when(requestValidator).validateGetLocationDeploymentHistoryRequest(request);
        getLocationDeploymentHistoryActivity.enact(request);
    }
}
