package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestRoute53SingleCustomerTemplateLocationsHelper {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testLocationsFromLocationsHelper() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        Route53SingleCustomerTemplateLocationsHelper helper = new Route53SingleCustomerTemplateLocationsHelper(locationsHelper);
        Set<String> nonBWLocations = Sets.newHashSet("POP1", "POP2", "POP3");
        when(locationsHelper.getAllNonBlackwatchPOPs()).thenReturn(nonBWLocations);
        
        MitigationModificationRequest request = DDBBasedCreateRequestStorageHandlerTest.createMitigationModificationRequest();
        request.setLocation(Lists.newArrayList("SomePOP1", "SomePOP2"));
        
        Set<String> locations = helper.getLocationsForDeployment(request);
        assertNotNull(locations);
        assertTrue(locations.size() == nonBWLocations.size());
        assertEquals(locations, nonBWLocations);
    }
}
