package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestTestHelper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TemplateBasedLocationsHelperManagerTest {
    private static final List<String> locations = Arrays.asList("G-IAD55", "G-IAD5");
    
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    @Test
    public void testGetLocationsForDeploymentBasedOnRequest() {
        EdgeLocationsHelper locationsHelper = mock(EdgeLocationsHelper.class);
        TemplateBasedLocationsManager manager = new TemplateBasedLocationsManager(mock(BlackWatchTemplateLocationHelper.class));
        
        CreateMitigationRequest request = 
                RequestTestHelper.generateCreateMitigationRequest(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, "Name");
        List<String> requestLocations = Lists.newArrayList("SomePOP1", "SomePOP2");
        request.setLocations(requestLocations);
        request.setMitigationTemplate("SomeRandomTemplate");
        
        Set<String> locations = manager.getLocationsForDeployment(request, mock(TSDMetrics.class));
        assertNotNull(locations);
        assertEquals(locations, Sets.newHashSet(requestLocations));
    }
     
    @Test(expected = IllegalArgumentException.class)
    public void testGetLocationsForCreateRequestWithNoLocation() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        TemplateBasedLocationsManager.getLocationsFromRequest(request);
    }
    
    @Test
    public void testGetLocationsForCreateRequestWithLocation() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setLocations(locations);
        assertEquals(new HashSet<>(locations), TemplateBasedLocationsManager.getLocationsFromRequest(request));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetLocationsForEditRequestWithNoLocation() {
        EditMitigationRequest request = new EditMitigationRequest();
        TemplateBasedLocationsManager.getLocationsFromRequest(request);
    }
    
    @Test
    public void testGetLocationsForEditRequestWithLocation() {
        EditMitigationRequest request = new EditMitigationRequest();
        request.setLocations(locations);
        assertEquals(new HashSet<>(locations), TemplateBasedLocationsManager.getLocationsFromRequest(request));
    }

    @Test
    public void testGetLocationsForDeleteRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        assertEquals(null, TemplateBasedLocationsManager.getLocationsFromRequest(request));
    }

    @Test
    public void testGetLocationsForRollbackRequest() {
        RollbackMitigationRequest request = new RollbackMitigationRequest();
        assertEquals(null, TemplateBasedLocationsManager.getLocationsFromRequest(request));
    }
}
