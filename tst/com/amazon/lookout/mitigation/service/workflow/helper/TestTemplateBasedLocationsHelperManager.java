package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestTemplateBasedLocationsHelperManager {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testGetLocationsForDeploymentBasedOnTemplate() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        Route53SingleCustomerTemplateLocationsHelper helper = new Route53SingleCustomerTemplateLocationsHelper(locationsHelper);
        TemplateBasedLocationsHelperManager manager = new TemplateBasedLocationsHelperManager(helper);
        
        Set<String> nonBWLocations = Sets.newHashSet("POP1", "POP2", "POP3");
        when(locationsHelper.getAllNonBlackwatchPOPs()).thenReturn(nonBWLocations);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        List<String> requestLocations = Lists.newArrayList("SomePOP1", "SomePOP2");
        request.setLocation(requestLocations);
        
        Set<String> locations = manager.getLocationsForDeployment(request);
        assertNotNull(locations);
        assertEquals(locations, nonBWLocations);
    }
    
    @Test
    public void testGetLocationsForDeploymentBasedOnRequest() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        Route53SingleCustomerTemplateLocationsHelper helper = new Route53SingleCustomerTemplateLocationsHelper(locationsHelper);
        TemplateBasedLocationsHelperManager manager = new TemplateBasedLocationsHelperManager(helper);
        
        Set<String> nonBWLocations = Sets.newHashSet("POP1", "POP2", "POP3");
        when(locationsHelper.getAllNonBlackwatchPOPs()).thenReturn(nonBWLocations);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        List<String> requestLocations = Lists.newArrayList("SomePOP1", "SomePOP2");
        request.setLocation(requestLocations);
        request.setMitigationTemplate("SomeRandomTemplate");
        
        Set<String> locations = manager.getLocationsForDeployment(request);
        assertNotNull(locations);
        assertEquals(locations, Sets.newHashSet(requestLocations));
    }
}
