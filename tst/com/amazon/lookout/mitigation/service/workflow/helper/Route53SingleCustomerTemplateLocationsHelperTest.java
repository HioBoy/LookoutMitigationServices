package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandlerTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Route53SingleCustomerTemplateLocationsHelperTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testLocationsFromLocationsHelper() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        Set<String> ciscoLocations = Sets.newHashSet("POP1");
        Set<String> nonBWLocations = Sets.newHashSet("POP1", "POP2", "POP3");
        Route53SingleCustomerTemplateLocationsHelper helper = new Route53SingleCustomerTemplateLocationsHelper(locationsHelper, ciscoLocations);
        when(locationsHelper.getAllNonBlackwatchPOPs()).thenReturn(nonBWLocations);
        
        CreateMitigationRequest request = DDBBasedCreateRequestStorageHandlerTest.generateCreateMitigationRequest();
        request.setLocations(Lists.newArrayList("SomePOP1", "SomePOP2"));
        
        Set<String> locations = helper.getLocationsForDeployment(request);
        assertNotNull(locations);
        assertTrue(locations.size() == (nonBWLocations.size() - ciscoLocations.size()));
        assertEquals(locations, Sets.difference(nonBWLocations, ciscoLocations));
    }
}
