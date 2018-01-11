package com.amazon.lookout.mitigation.service.activity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.constants.DeviceName;

public class ListActiveMitigationsForServiceActivityTest extends ActivityTestHelper {
    private static final String location = "location1";
    private static final DeviceName device = DeviceName.ANY_DEVICE;
    private static final String service = "service1";
    private ListActiveMitigationsForServiceRequest request;

    private ListActiveMitigationsForServiceActivity listActiveMitigationsForServiceActivity;

    @Before
    public void setup() {
        listActiveMitigationsForServiceActivity =
                spy(new ListActiveMitigationsForServiceActivity(
                        requestValidator, activeMitigationsHelper));
        
        request = new ListActiveMitigationsForServiceRequest();
        request.setLocation(location);
        request.setDeviceName(device.name());
    }
    
    @Test
    public void testListActiveMitigations() {
        Mockito.doNothing().when(requestValidator).validateListActiveMitigationsForServiceRequest(request);
        Mockito.doReturn(requestId).when(listActiveMitigationsForServiceActivity).getRequestId();
        
        List<MitigationRequestDescriptionWithStatuses> descriptionWithStatuses = 
                new ArrayList<>();

        MitigationActionMetadata actionMetadata = 
                MitigationActionMetadata.builder()
                .withDescription("is on fire")
                .withToolName("fire extinguisher")
                .withUser("fire fighter")
                .withRelatedTickets(Arrays.asList("help", "me")).build();
        
        MitigationRequestDescription requestDescription = 
                MitigationRequestDescription.builder()
                .withDeviceName(device.name())
                .withJobId(1234)
                .withMitigationName("mitigation_name")
                .withMitigationVersion(23)
                .withMitigationDefinition(new MitigationDefinition())
                .withMitigationActionMetadata(actionMetadata)
                .withDeployDate(123432442L)
                .build();

        MitigationRequestDescriptionWithStatuses mitigationRequestDescriptionWithStatuses =
                MitigationRequestDescriptionWithStatuses.builder()
                .withMitigationRequestDescription(requestDescription).build();
        
        descriptionWithStatuses.add(mitigationRequestDescriptionWithStatuses);
        
        Mockito.doReturn(descriptionWithStatuses).when(activeMitigationsHelper)
                .getActiveMitigations(eq(device), eq(location));
        
        ListActiveMitigationsForServiceResponse response = 
                listActiveMitigationsForServiceActivity.enact(request);
        
        assertEquals(descriptionWithStatuses, response.getMitigationRequestDescriptionsWithStatuses());
        assertEquals(requestId, response.getRequestId());
    }
    
    /**
     * Invalid request
     */
    @Test(expected = BadRequest400.class)
    public void testInvalidRequest() {
        Mockito.doThrow(new IllegalArgumentException())
                .when(requestValidator).validateListActiveMitigationsForServiceRequest(request);
        
        listActiveMitigationsForServiceActivity.enact(request);
    }
}
