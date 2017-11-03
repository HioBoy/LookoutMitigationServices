
package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.helper.BlackWatchHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchELBResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchIPAddressListResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchIPAddressResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.ELBResourceHelper;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.blackwatch.mitigation.state.model.ELBBriefInformation;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationStateSetting;
import com.amazon.blackwatch.mitigation.state.model.ResourceAllocationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.elb.internal.AccessPointNotFoundException;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchResourceTypeValidator;
import com.amazon.blackwatch.mitigation.resource.validator.ELBResourceTypeValidator;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressListResourceTypeValidator;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressResourceTypeValidator;
import com.amazon.lookout.mitigation.service.*;
import com.amazon.lookout.mitigation.service.workflow.helper.DogFishMetadataProvider;
import com.amazon.lookout.mitigation.service.workflow.helper.DogFishValidationHelper;
import com.amazon.lookout.models.prefixes.DogfishIPPrefix;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class DDBBasedBlackWatchMitigationInfoHandlerTest {
    
    //@Rule for exception handling is more flexible than the entire test expecting the exception
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    private static final String domain = "unit-test-domain";
    private static final String realm = "unit-test-realm";

    private static AmazonDynamoDBClient dynamoDBClient;
    private static DDBBasedBlackWatchMitigationInfoHandler blackWatchMitigationInfoHandler;
    private static ResourceAllocationStateDynamoDBHelper resourceAllocationStateDDBHelper;
    private static ResourceAllocationHelper resourceAllocationHelper;
    private static MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper;
    private static ELBResourceHelper elbResourceHelper;

    private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    private static Metrics metrics = Mockito.mock(Metrics.class);
    private static TSDMetrics tsdMetrics;

    private static final String testMitigation1 = "testMitigation-20160818";
    private static final String testMitigation2 = "testMitigation-20160819";
    private static final String testMasterRegion = "us-east-1";
    private static final String testSecondaryRegion = "us-west-1";
    private static final Map<String, String> endpointMap = ImmutableMap.of(testMasterRegion, "master-1.a.c", 
            testSecondaryRegion, "secondary-1.a.c");

    private static final String testOwnerARN1 = "testOwnerARN1";
    private static final String testOwnerARN2 = "testOwnerARN2";
    private static final long testChangeTime = 151515L;
    private static final int testMinsToLive = 100;
    private static final String testIPAddressResourceId = "1.2.3.4";
    private static final String testIPAddressResourceId2 = "4.3.2.1";
    private static final String testIPAddressResourceIdCanonical = "1.2.3.4/32";
    private static final String testIPAddressResourceId2Canonical = "4.3.2.1/32";
    private static final String testIPv6AddressResourceId = "0000::1";
    private static final String testIPv6AddressResourceIdCanonical = "::1/128";
    private static final String testIPAddressResourceType = BlackWatchMitigationResourceType.IPAddress.name();
    private static final String testIPAddressListResourceType = BlackWatchMitigationResourceType.IPAddressList.name();
    private static final String testELBResourceType = BlackWatchMitigationResourceType.ELB.name();
    private static final String testLocation = "BR-SFO5-1";
    private static final String testValidJSON = "{ }";
    //Generated with: echo -n $ESCAPED_STRING | sha256sum
    private static final String validJSONChecksum = "257c1be96ae69f4b01c2c69bdb6d78605f59175819fb007d0bf245bf48444c4a";
    private static final String ipListTemplate = "{\"destinations\":[%s]}";
    private static Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeHelper> resourceTypeHelpers;
    
    private static BlackWatchMitigationActionMetadata testBWMetadata;
    private static MitigationActionMetadata testMetadata;
    private static Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator> resourceTypeValidatorMap
        = new HashMap<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator>();
    
    private static final Map<String, Set<String>> recordedResourcesMap = new HashMap<String, Set<String>>();
    private static final MitigationStateSetting setting = new MitigationStateSetting();  
    private static final Map<String, MitigationStateSetting> locationMitigationState = 
            new HashMap<String, MitigationStateSetting> ();
    private static DogFishMetadataProvider dogfishProvider;
    private static DogFishValidationHelper dogfishValidator;
    
    private static MitigationState mitigationState1;
    private static MitigationState mitigationState2;

    @Before
    public void beforeTests() {
        mitigationStateDynamoDBHelper.deleteTable();
        mitigationStateDynamoDBHelper.createTableIfNotExist(5L, 5L);
        resourceAllocationStateDDBHelper.deleteTable();
        resourceAllocationStateDDBHelper.createTableIfNotExist(5L, 5L);
        
        //reset the Dogfish mock
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testMasterRegion);

        Mockito.doReturn(prefix).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        dogfishValidator = new DogFishValidationHelper(testMasterRegion, testMasterRegion, dogfishProvider, endpointMap);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, resourceAllocationHelper, dogfishValidator, 
                resourceTypeValidatorMap, resourceTypeHelpers,  4, "us-east-1");
    }
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();
        tsdMetrics = new TSDMetrics(metricsFactory);
        elbResourceHelper = Mockito.mock(ELBResourceHelper.class);
        dogfishProvider = Mockito.mock(DogFishMetadataProvider.class);
        dogfishValidator = new DogFishValidationHelper(testMasterRegion, testMasterRegion, dogfishProvider, endpointMap);

        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddress, 
                new IPAddressResourceTypeValidator());
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddressList, 
                new IPAddressListResourceTypeValidator());
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.ELB, 
                new ELBResourceTypeValidator());

        
        BlackWatchELBResourceTypeHelper elbResourceTypeHelper =  new BlackWatchELBResourceTypeHelper(elbResourceHelper, metricsFactory);
        BlackWatchIPAddressResourceTypeHelper ipadressResourceTypeHelper =  new BlackWatchIPAddressResourceTypeHelper();
        BlackWatchIPAddressListResourceTypeHelper ipaddressListResourceTypeHelper =  new BlackWatchIPAddressListResourceTypeHelper();
        resourceTypeHelpers = ImmutableMap.of(
                BlackWatchMitigationResourceType.IPAddress, ipadressResourceTypeHelper,
                BlackWatchMitigationResourceType.IPAddressList, ipaddressListResourceTypeHelper,
                BlackWatchMitigationResourceType.ELB, elbResourceTypeHelper
                );
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        mitigationStateDynamoDBHelper = new MitigationStateDynamoDBHelper(dynamoDBClient, realm, domain, 
                5L, 5L, metricsFactory);
        resourceAllocationStateDDBHelper = new ResourceAllocationStateDynamoDBHelper(dynamoDBClient, realm, domain, 
                5L, 5L, metricsFactory);
        resourceAllocationHelper = new ResourceAllocationHelper(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, metricsFactory);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, resourceAllocationHelper, dogfishValidator, resourceTypeValidatorMap, resourceTypeHelpers, 4, "us-east-1");

        BlackWatchMitigationResourceType testblackWatchIPAddressResourceType = BlackWatchMitigationResourceType.valueOf(testIPAddressResourceType);

        
        recordedResourcesMap.put("ELB", ImmutableSet.of("ELB-18181"));
        recordedResourcesMap.put("EC2", ImmutableSet.of("EC2-55858"));
               
        testMetadata = MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build();
        
        testBWMetadata = BlackWatchMitigationActionMetadata.builder()
                .user(testMetadata.getUser())
                .toolName(testMetadata.getToolName())
                .description(testMetadata.getDescription())
                .relatedTickets(testMetadata.getRelatedTickets())
                .build();

        setting.setMitigationSettingsJSONChecksum("234534dfgdfg3344");
        setting.setPPS(151515L);
        setting.setBPS(121212L);
        locationMitigationState.put(testLocation, setting);
        mitigationState1 = MitigationState.builder()
                .mitigationId(testMitigation1)
                .resourceId(testIPAddressResourceIdCanonical)
                .resourceType(testIPAddressResourceType)
                .changeTime(testChangeTime)
                .ownerARN(testOwnerARN1)
                .recordedResources(recordedResourcesMap)
                .locationMitigationState(locationMitigationState)
                .state(MitigationState.State.Active.name())
                .ppsRate(1121L)
                .bpsRate(2323L)
                .mitigationSettingsJSON("{\"mitigation_config\": {\"ip_validation\": {\"action\": \"DROP\"}}}")
                .mitigationSettingsJSONChecksum("ABABABABA")
                .minutesToLive(testMinsToLive)
                .latestMitigationActionMetadata(testBWMetadata)
                .build();
        
        mitigationState2 = MitigationState.builder()
                .mitigationId(testMitigation2)
                .resourceId(testIPAddressResourceId2Canonical)
                .resourceType(testIPAddressResourceType)
                .changeTime(23232L)
                .ownerARN(testOwnerARN2)
                .recordedResources(recordedResourcesMap)
                .locationMitigationState(locationMitigationState)
                .state(MitigationState.State.Active.name())
                .ppsRate(1121L)
                .bpsRate(2323L)
                .mitigationSettingsJSON("{\"mitigation_config\": {\"ip_validation\": {\"action\": \"DROP\"}}}")
                .mitigationSettingsJSONChecksum("ABABABABA")
                .minutesToLive(100)
                .latestMitigationActionMetadata(BlackWatchMitigationActionMetadata.builder()
                        .user("Khaleesi")
                        .toolName("JUnit")
                        .description("Test Descr")
                        .relatedTickets(Arrays.asList("1234", "5655"))
                        .build())
                .build();
    }
    
    private BlackWatchTargetConfig parseJSON(String json) {
        try {
            return BlackWatchTargetConfig.fromJSONString(json);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse JSON: " + e.getMessage(), e);
        }
    }

    private void validateMitigation(BlackWatchMitigationDefinition bwMitigationDefinition, MitigationState ms) {
        assertEquals(bwMitigationDefinition.getMitigationId(), ms.getMitigationId());
        assertEquals(bwMitigationDefinition.getResourceId(), ms.getResourceId());
        assertEquals(bwMitigationDefinition.getResourceType(), ms.getResourceType());
        assertEquals(bwMitigationDefinition.getState(), ms.getState());
        assertEquals(bwMitigationDefinition.getChangeTime(), ms.getChangeTime().longValue());
        assertEquals(bwMitigationDefinition.getGlobalBPS(), ms.getBpsRate());
        assertEquals(bwMitigationDefinition.getGlobalPPS(), ms.getPpsRate());
        assertEquals(bwMitigationDefinition.getMinutesToLiveAtChangeTime(), ms.getMinutesToLive().longValue());
        assertEquals(bwMitigationDefinition.getOwnerARN(), ms.getOwnerARN());
        assertEquals(bwMitigationDefinition.getMitigationSettingsJSON(), ms.getMitigationSettingsJSON());
        assertEquals(bwMitigationDefinition.getMitigationSettingsJSONChecksum(), ms.getMitigationSettingsJSONChecksum());
        assertEquals(bwMitigationDefinition.getRecordedResources(), ms.getRecordedResources());
        assertEquals(bwMitigationDefinition.getLatestMitigationActionMetadata().getUser(), ms.getLatestMitigationActionMetadata().getUser());
        assertEquals(bwMitigationDefinition.getLatestMitigationActionMetadata().getToolName(), ms.getLatestMitigationActionMetadata().getToolName());
        assertEquals(bwMitigationDefinition.getLatestMitigationActionMetadata().getDescription(), ms.getLatestMitigationActionMetadata().getDescription());
        assertEquals(bwMitigationDefinition.getLatestMitigationActionMetadata().getRelatedTickets(), ms.getLatestMitigationActionMetadata().getRelatedTickets());
        
        Map<String, LocationMitigationStateSettings> locationMitigationStateSettings = bwMitigationDefinition.getLocationMitigationState();
        Map<String, MitigationStateSetting> locationMitigationStateSettingsExpected = ms.getLocationMitigationState();
        assertEquals(locationMitigationStateSettings.keySet(), locationMitigationStateSettingsExpected.keySet());
        for (String key : locationMitigationStateSettings.keySet()) {
            assertEquals(locationMitigationStateSettings.get(key).getBPS(), locationMitigationStateSettingsExpected.get(key).getBPS());
            assertEquals(locationMitigationStateSettings.get(key).getPPS(), locationMitigationStateSettingsExpected.get(key).getPPS());
            assertEquals(locationMitigationStateSettings.get(key).getMitigationSettingsJSONChecksum(), locationMitigationStateSettingsExpected.get(key).getMitigationSettingsJSONChecksum());
        }
    }

    @Test
    public void testDeactivateBlackWatchMitigation() {
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId(), requestMetadata);
        MitigationState newMitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationState1.getMitigationId());
        assertEquals(MitigationState.State.To_Delete.name(), newMitigationState.getState());
        assertEquals(requestMetadata.getUser(), newMitigationState.getLatestMitigationActionMetadata().getUser());
        assertEquals(requestMetadata.getToolName(), newMitigationState.getLatestMitigationActionMetadata().getToolName());
        assertEquals(requestMetadata.getDescription(), newMitigationState.getLatestMitigationActionMetadata().getDescription());
        assertEquals(requestMetadata.getRelatedTickets(), newMitigationState.getLatestMitigationActionMetadata().getRelatedTickets());
    }

    @Test
    public void testDeactivateBlackWatchMitigationFail() {
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        mitigationState1.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId(), requestMetadata);
        mitigationState1.setState(MitigationState.State.Active.name());
    }

    @Test
    public void testDeactivateBlackWatchMitigationNonExistent() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        try {
            blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId() + "Fail", requestMetadata);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        mitigationState1.setState(MitigationState.State.Active.name());
    }

    @Test
    public void testChangeOwnerARN() {
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        String newOwnerARN  = "new" + testOwnerARN1;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.changeOwnerARN(mitigationState1.getMitigationId(), newOwnerARN, testOwnerARN1, requestMetadata);
        MitigationState newMitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationState1.getMitigationId());
        assertEquals(newOwnerARN, newMitigationState.getOwnerARN());
        assertEquals(requestMetadata.getUser(), newMitigationState.getLatestMitigationActionMetadata().getUser());
        assertEquals(requestMetadata.getToolName(), newMitigationState.getLatestMitigationActionMetadata().getToolName());
        assertEquals(requestMetadata.getDescription(), newMitigationState.getLatestMitigationActionMetadata().getDescription());
        assertEquals(requestMetadata.getRelatedTickets(), newMitigationState.getLatestMitigationActionMetadata().getRelatedTickets());
    }

    @Test
    public void testChangeOwnerARNFail() {
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        String newOwnerARN  = "new" + testOwnerARN1;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.changeOwnerARN(mitigationState1.getMitigationId(), newOwnerARN, newOwnerARN, requestMetadata);
        } catch (ConditionalCheckFailedException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testChangeOwnerARNFailNonExistant() {
        MitigationActionMetadata requestMetadata = MitigationActionMetadata.builder()
            .withUser("Khaleesi_update")
            .withToolName("JUnit_update")
            .withDescription("Test Descr_update")
            .withRelatedTickets(Arrays.asList("4321"))
            .build();
        String newOwnerARN  = "new" + testOwnerARN1;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.changeOwnerARN("Fail" + mitigationState1.getMitigationId(), newOwnerARN, newOwnerARN, requestMetadata);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testGetBlackWatchMitigationsIPNormalize() {
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
            testIPv6AddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(applyResponse);
        assertNotEquals(testIPv6AddressResourceId, testIPv6AddressResourceIdCanonical);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);

        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(null, null, null, null, 5, tsdMetrics);
        assertEquals(1, listOfBlackwatchMitigation.size());
        assertEquals(testIPv6AddressResourceIdCanonical, listOfBlackwatchMitigation.get(0).getResourceId());

        //Try and find using non-canonical form (DB has canonical as verified above)

        //List using non-canonical Id, with resourceType
        listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(null, testIPv6AddressResourceId, testIPAddressResourceType, null, 5, tsdMetrics);
        assertEquals(1, listOfBlackwatchMitigation.size());
        assertEquals(testIPv6AddressResourceIdCanonical, listOfBlackwatchMitigation.get(0).getResourceId());

        //List using non-canonical Id, without resourceType
        listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(null, testIPv6AddressResourceId, null, null, 5, tsdMetrics);
        assertEquals(1, listOfBlackwatchMitigation.size());
        assertEquals(testIPv6AddressResourceIdCanonical, listOfBlackwatchMitigation.get(0).getResourceId());
    }

    @Test
    public void testGetBlackWatchMitigationsNoFilter() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2));
        //no filter, should return all mitigations
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 2);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
        validateMitigation(listOfBlackwatchMitigation.get(1), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByMitigationId() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        
        //filter by mitigation id
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, null, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterByResourceId() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource id
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(null, testIPAddressResourceId2, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterByResourceType() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource type, both mitigations match the resource type, return both
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(null, null, testIPAddressResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 2);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
        validateMitigation(listOfBlackwatchMitigation.get(1), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterOwnerARN() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource type, both mitigations match the resource type, return both
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation 
        = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, testOwnerARN1, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsExpiryTime() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource type, both mitigations match the resource type, return both
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = 
                blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, testOwnerARN1, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        BlackWatchMitigationDefinition retState = listOfBlackwatchMitigation.get(0);
        validateMitigation(retState, mitigationState1);
        assertEquals(retState.getExpiryTime(), testChangeTime + (testMinsToLive * 60 * 1000));
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptions() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by mitigation id, resource id, resource type, only one match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = 
                blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, testIPAddressResourceId,
                        testIPAddressResourceType, testOwnerARN1, 5, tsdMetrics);
        assertEquals(1, listOfBlackwatchMitigation.size());
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptionsNoMatch() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2));  
        //filter by mitigation id, resource id, resource type, no match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler
            .getBlackWatchMitigations(testMitigation1, testIPAddressResourceId2, testIPAddressResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 0);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByMaxEntry() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //set max # of entries to return to be 1, we should only return 1 mitigaiton. 
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, null, 1, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testChecksumString() {
        assertEquals("674c1f08fac053e604366eb24f2123568e367479301d4dd14e6109ca85abda1b",
                BlackWatchHelper.getHexStringChecksum("Bryan"));
        assertNull(BlackWatchHelper.getHexStringChecksum(null));
    }

    @Test
    public void testUpdateBlackWatchMitigationSuccess() {
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(applyResponse);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);
        
        UpdateBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                applyResponse.getMitigationId(), 30, testMetadata, parseJSON(testValidJSON), "ARN-1122", tsdMetrics);
        assertNotNull(response);
    }
    
    @Test
    public void testUpdateBlackWatchMitigationFailedNoMit() {
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                "NotThere", 30, testMetadata, parseJSON(testValidJSON), "ARN-1122", tsdMetrics);
    }

    private Long getDefaultPps(MitigationState mitigation) {
        BlackWatchTargetConfig targetConfig = parseJSON(mitigation.getMitigationSettingsJSON());

        return targetConfig
            .getMitigation_config()
            .getGlobal_traffic_shaper()
            .get("default")
            .getGlobal_pps();
    }

    @Test
    public void testUpdateRateLimit() {
        final String configTemplate = ""
                + "{"
                + "  \"mitigation_config\": {"
                + "    \"ip_validation\": {"
                + "      \"action\": \"DROP\""
                + "    },"
                + "    \"global_traffic_shaper\": {"
                + "      \"default\": {"
                + "        \"global_pps\": %d"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        final Long oldRateLimit = 1112L;
        final Long newRateLimit = 9992L;

        final String oldConfig = String.format(configTemplate, oldRateLimit);
        final String newConfig = String.format(configTemplate, newRateLimit);

        final String testARN = "ARN-1222";

        // Apply
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata,
                parseJSON(oldConfig), testARN, tsdMetrics);
        assertNotNull(applyResponse);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);

        // Test that the rate limit was set initially
        final String mitId = applyResponse.getMitigationId();
        final MitigationState beforeState = mitigationStateDynamoDBHelper.getMitigationState(mitId);
        assertEquals(oldRateLimit, getDefaultPps(beforeState));

        // Update
        UpdateBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                mitId, 30, testMetadata, parseJSON(newConfig), testARN, tsdMetrics);
        assertNotNull(response);

        // Test that the rate limit was updated
        final MitigationState afterState = mitigationStateDynamoDBHelper.getMitigationState(mitId);
        assertEquals(newRateLimit, getDefaultPps(afterState));
    }

    @Test
    public void testUpdateBlackWatchMitigationFailedBadState() {
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(applyResponse);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);
        
        MitigationState mitState = mitigationStateDynamoDBHelper.getMitigationState(applyResponse.getMitigationId());
        mitState.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.updateMitigationState(mitState);
        
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                "NotThere", 30, testMetadata, parseJSON(testValidJSON), "ARN-1122", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationNewIPAddressSuccess() {
        
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        assertTrue(response.getMitigationId().length() > 0);
        
        ResourceAllocationState ras = resourceAllocationStateDDBHelper.getResourceAllocationState(testIPAddressResourceIdCanonical);
        assertNotNull(ras);
        assertEquals(response.getMitigationId(), ras.getMitigationId());
        assertEquals(ras.getResourceType(), testIPAddressResourceType);
        assertTrue(ras.getConfirmed());
        
        MitigationState mitState = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(mitState);
        assertEquals(testIPAddressResourceIdCanonical, mitState.getResourceId());
        assertEquals(testIPAddressResourceType, mitState.getResourceType());
        assertTrue(30 == mitState.getMinutesToLive());
        assertEquals(testBWMetadata, mitState.getLatestMitigationActionMetadata());
        assertEquals(testValidJSON, mitState.getMitigationSettingsJSON());
        assertEquals(validJSONChecksum, mitState.getMitigationSettingsJSONChecksum());
        assertEquals("ARN-1222", mitState.getOwnerARN());
        Map<String, Set<String>> rrs = mitState.getRecordedResources();
        assertNotNull(rrs);
        assertEquals(1, rrs.size());
        assertEquals(ImmutableSet.of(testIPAddressResourceIdCanonical), rrs.get(testIPAddressResourceType));
    }
    
    @Test
    public void testApplyBlackWatchMitigationNotNewSuccess() {
        
        ApplyBlackWatchMitigationResponse response1 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        assertNotNull(response1);
        assertTrue(response1.isNewMitigationCreated());
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        
        MitigationState mitState1 = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        assertNotNull(mitState1);
        assertEquals(testIPAddressResourceIdCanonical, mitState1.getResourceId());
        assertEquals(testIPAddressResourceType, mitState1.getResourceType());
        assertTrue(30 == mitState1.getMinutesToLive());
        assertEquals(testBWMetadata, mitState1.getLatestMitigationActionMetadata());
        assertEquals(testValidJSON, mitState1.getMitigationSettingsJSON());
        assertEquals(validJSONChecksum, mitState1.getMitigationSettingsJSONChecksum());
        assertEquals("ARN-1222", mitState1.getOwnerARN());
        Map<String, Set<String>> rrs = mitState1.getRecordedResources();
        assertNotNull(rrs);
        assertEquals(rrs.size(), 1);
        assertEquals(rrs.get(testIPAddressResourceType), ImmutableSet.of(testIPAddressResourceIdCanonical));
        
        ApplyBlackWatchMitigationResponse response2 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        assertNotNull(response2);
        assertFalse(response2.isNewMitigationCreated());
        assertEquals(mitigationId, response2.getMitigationId());
        
        MitigationState mitState2 = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        assertNotEquals(mitState1.getChangeTime(), mitState2.getChangeTime());
        assertNotEquals(mitState1.getVersionNumber(), mitState2.getVersionNumber());
        //Reset the only fields we expect to change from a second Apply call.
        mitState2.setChangeTime(mitState1.getChangeTime());
        mitState2.setVersionNumber(mitState1.getVersionNumber());
        assertEquals(mitState1, mitState2);
        
    }
    
    @Test
    public void testApplyBlackWatchMitigationDiffARN() {
        
        ApplyBlackWatchMitigationResponse response1 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        assertNotNull(response1);
        assertTrue(response1.isNewMitigationCreated());
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        
        thrown.expect(MitigationNotOwnedByRequestor400.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(testIPAddressResourceId, 
                    testIPAddressResourceType, 30, testMetadata, parseJSON(testValidJSON), "ARN-1255", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationBadType() {
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, "BADType", 30, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationOverrideMinstoLive() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, null, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertTrue(ms.getMinutesToLive() > 0);
        
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 0, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        assertTrue(ms.getMinutesToLive() > 0);
    }
    
    @Test
    public void testApplyBlackWatchMitigationMissingTableEntries() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(ms);
        mitigationStateDynamoDBHelper.deleteMitigationState(ms);
        
        thrown.expect(IllegalArgumentException.class);
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationMismatchType() {
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, "MismatchType", 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInactiveFailure() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        ms.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.updateMitigationState(ms);
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Could not save MitigationState due to conditional failure!");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddress() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address or CIDR!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }

    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressCIDR() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address or CIDR!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4/31", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }

    @Test
    public void testApplyBlackWatchMitigationValidIPAddressCIDR() {
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
            "1.2.3.4/32", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
            "ARN-1222", tsdMetrics);
    }

    @Test
    public void testApplyBlackWatchMitigationInvalidV6IPAddress() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address or CIDR!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation("2001:0db8:85a3:BBZZ:0000:8a2e:0370:7334", 
                testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidAddresses() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 
                testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListMissing() {
        //Non existent json
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Failed to parse");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata, parseJSON("{\"BBTest\":\"[]\"}"),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListEmpty() {
        //Blank array
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("IP List with one or more entries");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressListWithCIDR() {
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate,"\"1.2.3.4/32\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListBadEntry() {
        //invalid entry
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("1.2.3.Z");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate,"\"1.2.3.Z\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListBadEntryWithin() {
        //Mixed invalid
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("1.2.5.X");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"1.2.3.4\",\"1.2.5.X\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationIPAddressListDuplicates() {
        //Duplicate addresses specified.
        //They are silently ignored though
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"1.2.3.4\",\"5.5.4.4\",\"1.2.3.4\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressListV6Equivalent() {
        //IPV6 addresses that are different strings, but are equivalent.
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\"," +
                     "\"2001:0db8:85a3::8a2e:0370:7334\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressList() {
        //Valid
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"1.2.3.4\"")), "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        
        //Valid V6 + V4.
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\", \"1.2.3.4\"")), 
                "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertFalse(response.isNewMitigationCreated());
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressListDogFishFailed() {
        //Not found IPList
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("not found in DogFish");
        Mockito.doReturn(null).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 10, testMetadata,
                parseJSON(String.format(ipListTemplate, "\"1.2.3.4\"")), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressDogFishFailed() {
        //Not found IP Address
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("not found in DogFish");
        Mockito.doReturn(null).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressDogFishFailedOtherActive() {
        //DogFish returned active Mit SVC region.
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testSecondaryRegion);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("with an active mitigation service");
        Mockito.doReturn(prefix).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressDogFishFailedNotPrimaryUnknown() {
        //DogFish returned Non Mit SVC region - but we aren't primary.
        dogfishValidator = new DogFishValidationHelper(testSecondaryRegion, testMasterRegion, 
                dogfishProvider, endpointMap);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, resourceAllocationHelper, dogfishValidator, 
                resourceTypeValidatorMap, resourceTypeHelpers, 4, "us-east-1");
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion("NotActive");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("without an active mitigation service");
        Mockito.doReturn(prefix).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressDogFishFailedNotPrimary() {
        //DogFish returned Non Mit SVC region - but we aren't primary.
        dogfishValidator = new DogFishValidationHelper(testSecondaryRegion, testMasterRegion, 
                dogfishProvider, endpointMap);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, resourceAllocationHelper, dogfishValidator, 
                resourceTypeValidatorMap, resourceTypeHelpers, 4, "us-east-1");
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setRegion(testMasterRegion);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("with an active mitigation service");
        Mockito.doReturn(prefix).when(dogfishProvider).getCIDRMetaData(Mockito.anyString());
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 10, testMetadata, parseJSON(testValidJSON),
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationNewValidELBResource() {
        ELBBriefInformation elbBriefInformation = ELBBriefInformation.builder()
                .accessPointName("ELBAccessPointName")
                .accessPointVersion(2)
                .configVersion(1L)
                .dnsName("elb.dns.name")
                .state("Active")
                .build();
        Mockito.doReturn(elbBriefInformation).when(elbResourceHelper).getLoadBalancerBriefInformation(Mockito.anyLong());
        //Valid
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1234", testELBResourceType, 10, testMetadata,
                parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        assertEquals(state.getElbResourceConfiguration().getBriefInformation(), elbBriefInformation );
    }
    
    @Test
    public void testApplyBlackWatchMitigationExistValidELBResource() {
        String accessPointId = "1234";
        ELBBriefInformation elbBriefInformation = ELBBriefInformation.builder()
                .accessPointName("ELBAccessPointName")
                .accessPointVersion(2)
                .configVersion(1L)
                .dnsName("elb.dns.name")
                .state("Active")
                .build();
        Mockito.doReturn(elbBriefInformation).when(elbResourceHelper).getLoadBalancerBriefInformation(Mockito.anyLong());
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                accessPointId, testELBResourceType, 10, testMetadata,
                parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        MitigationState state = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        assertEquals(state.getElbResourceConfiguration().getBriefInformation(), elbBriefInformation );
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                accessPointId, testELBResourceType, 10, testMetadata,
                parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertFalse(response.isNewMitigationCreated());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testApplyBlackWatchMitigationNonExistELBResource() {
        Mockito.doThrow(AccessPointNotFoundException.class).when(elbResourceHelper).getLoadBalancerBriefInformation(Mockito.anyLong());
        //ELB resource doesn't exist, should throw exception.
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1234", testELBResourceType, 10, testMetadata,
                parseJSON(testValidJSON), "ARN-1222", tsdMetrics);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ValidateResource_InvalidARN() {
        Map<BlackWatchMitigationResourceType, Set<String>> resourceMap = new HashMap<>();
        resourceMap.put(BlackWatchMitigationResourceType.ElasticIP, Collections.singleton("foo"));
        blackWatchMitigationInfoHandler.validateResources(resourceMap);
    }

    @Test
    public void ValidateResource_ValidARN() {
        Map<BlackWatchMitigationResourceType, Set<String>> resourceMap = new HashMap<>();
        resourceMap.put(BlackWatchMitigationResourceType.ElasticIP, Collections.singleton("arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-abc12345"));
        blackWatchMitigationInfoHandler.validateResources(resourceMap);
    }
    
}

