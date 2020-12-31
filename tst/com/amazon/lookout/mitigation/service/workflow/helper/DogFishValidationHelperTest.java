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

    private static final String testRegion1 = "test-1";
    private static final String testRegion2 = "test-2";
    private static final String testRegion3 = "test-3";
    private static final String testRegion4 = "test-4";
    private static final String testRegion5 = "test-5";
    private static final String testRegion6 = "test-6";

    Map<String, String> regionEndpoints = ImmutableMap.of(
        testRegion1, "test-1.amazon.com",
        testRegion2, "test-2.amazon.com",
        testRegion4, "test-4.amazon.com",
        testRegion5, "test-5.amazon.com",
        testRegion6, "test-6.amazon.com"
    );
    Map<String, Boolean> handleActiveBWAPIMitigations = ImmutableMap.of(
        testRegion1, Boolean.TRUE,
        testRegion2, Boolean.TRUE,
        testRegion4, Boolean.FALSE,
        testRegion5, Boolean.TRUE,
        testRegion6, Boolean.TRUE
    );
    Map<String, Boolean> acceptMitigationsAtMasterRegion = ImmutableMap.of(
        testRegion1, Boolean.FALSE,
        testRegion2, Boolean.FALSE,
        testRegion4, Boolean.FALSE,
        testRegion5, Boolean.TRUE,
        testRegion6, Boolean.FALSE
    );

    private static final DogFishMetadataProvider dogFishProvider 
        = Mockito.mock(DogFishMetadataProvider.class);
    
    @Test
    public void dogFishValidationHelperInRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion1);
        
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperMasterRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion3);//region where an endpoint mapping doesn't exist
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperNotFoundTest() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("not found in DogFish");
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(null).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
    
    @Test
    public void dogFishValidationHelperNotMasterTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion3);//region where an endpoint mapping doesn't exist
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("without an active");
        helper = new DogFishValidationHelper(testRegion1, testRegion2, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }

    @Test
    public void dogFishValidationHelperRegionNotHandlingActiveMitigationsTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion4); // testRegion4 does not handle active mitigations, accept mitigations in master region
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }

    @Test
    public void dogFishValidationHelperRegionHandlingActiveMitigationsWithAcceptMitigationsAtMasterRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion5); // testRegion5 handleActiveBWAPIMitigations==TRUE, acceptMitigationsAtMasterRegion==TRUE
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }

    @Test
    public void dogFishValidationHelperRegionHandlingActiveMitigationsWithOutAcceptMitigationsAtMasterRegionTest() {
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testRegion6); // testRegion6 handleActiveBWAPIMitigations==TRUE, acceptMitigationsAtMasterRegion==FALSE
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("please use that regional endpoint to create a mitigation for the resource");
        helper = new DogFishValidationHelper(testRegion1, testRegion1, dogFishProvider, regionEndpoints,
            handleActiveBWAPIMitigations, acceptMitigationsAtMasterRegion);
        Mockito.doReturn(prefix).when(dogFishProvider).getCIDRMetaData(Mockito.anyString());
        helper.validateCIDRInRegion("1.2.3.4");
    }
}
