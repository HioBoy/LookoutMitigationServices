package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.amazon.lookout.models.prefixes.DogfishIPPrefix;
import com.google.common.collect.ImmutableMap;

public class DogFishValidationHelperTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    private static DogFishValidationHelper helper;
    Map<String, String> regionEndpoints = ImmutableMap.of(testRegion1, 
            "test-1.amazon.com", testRegion2, "test-2.amazon.com",
            testRegion4, "test-4.amazon.com");
    Map<String, Boolean> handleActiveBWAPIMitigations = ImmutableMap.of(testRegion1,
            Boolean.TRUE, testRegion2, Boolean.TRUE, testRegion4, Boolean.FALSE);
    private static final String testRegion1 = "test-1";
    private static final String testRegion2= "test-2";
    private static final String testRegion3= "test-3";
    private static final String testRegion4 = "test-4";

    private static final DogFishMetadataProvider dogFishProvider 
        = Mockito.mock(DogFishMetadataProvider.class);
    
    @Test
    public void dogFishValidationHelperInRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion1);
        
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints, handleActiveBWAPIMitigations);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperMasterRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion3);//region where an endpoint mapping doesn't exist
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints, handleActiveBWAPIMitigations);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperNotFoundTest() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("not found in DogFish");
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints, handleActiveBWAPIMitigations);
        Mockito.doReturn(null).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperNotMasterTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion3);//region where an endpoint mapping doesn't exist
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("without an active");
        helper = new DogFishValidationHelper(testRegion1, testRegion2, dogFishProvider, regionEndpoints, handleActiveBWAPIMitigations);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }

    @Test
    public void dogFishValidationHelperRegionNotHandlingActiveMitigationsTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion4); // testRegion4 does not handle active mitigations, accept mitigations in master region
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints, handleActiveBWAPIMitigations);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
}
