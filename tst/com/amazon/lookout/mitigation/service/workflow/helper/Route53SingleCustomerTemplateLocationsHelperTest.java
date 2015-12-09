package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Route53SingleCustomerTemplateLocationsHelperTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    @Test
    public void testLocationsFromLocationsHelper() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        Set<String> ciscoLocations = Sets.newHashSet("POP1");
        Set<String> nonBWLocations = Sets.newHashSet("POP1", "POP2", "POP3");
        Route53SingleCustomerTemplateLocationsHelper helper = new Route53SingleCustomerTemplateLocationsHelper(locationsHelper, ciscoLocations);
        when(locationsHelper.getAllNonBlackwatchClassicPOPs()).thenReturn(nonBWLocations);
        
        // RateLimit MitigationTemplate
        CreateMitigationRequest request = RequestTestHelper.generateCreateMitigationRequest(
                        MitigationTemplate.Router_RateLimit_Route53Customer, "Name");
        request.setLocations(Lists.newArrayList("SomePOP1", "SomePOP2"));
        
        Set<String> locations = helper.getLocationsForDeployment(request, mock(TSDMetrics.class));
        assertNotNull(locations);
        assertTrue(locations.size() == (nonBWLocations.size() - ciscoLocations.size()));
        assertEquals(locations, Sets.difference(nonBWLocations, ciscoLocations));
        
        // CountMode MitigationTemplate
        request = RequestTestHelper.generateCreateMitigationRequest(
                MitigationTemplate.Router_CountMode_Route53Customer, "Name");
        request.setLocations(Lists.newArrayList("SomePOP1", "SomePOP2"));
        
        locations = helper.getLocationsForDeployment(request, mock(TSDMetrics.class));
        assertNotNull(locations);
        assertTrue(locations.size() == (nonBWLocations.size() - ciscoLocations.size()));
        assertEquals(locations, Sets.difference(nonBWLocations, ciscoLocations));
    }
}
