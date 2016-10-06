
package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.blackwatch.model.BlackWatchMitigationResourceType;
import com.amazon.lookout.mitigation.blackwatch.model.BlackWatchResourceTypeValidator;
import com.amazon.lookout.mitigation.blackwatch.model.IPAddressListResourceTypeValidator;
import com.amazon.lookout.mitigation.blackwatch.model.IPAddressResourceTypeValidator;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationStateSetting;
import com.amazon.blackwatch.mitigation.state.model.ResourceAllocationState;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.google.common.collect.ImmutableSet;

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

    private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    private static Metrics metrics = Mockito.mock(Metrics.class);
    private static TSDMetrics tsdMetrics;

    private static final String testMitigation1 = "testMitigation-20160818";
    private static final String testMitigation2 = "testMitigation-20160819";
    private static final String testResourceId2 = "TEST-RESOURCE-2";

    private static final String testOwnerARN = "testOwnerARN";
    private static final String testResourceId1 = "TEST-RESOURCE-1";
    private static final String testIPAddressResourceId = "1.2.3.4";
    private static final String testIPAddressResourceType = BlackWatchMitigationResourceType.IPAddress.name();
    private static final String testIPAddressListResourceType = BlackWatchMitigationResourceType.IPAddressList.name();
    private static final String testLocation = "BR-SFO5-1";
    private static final String testResourceType = "testResourceType";
    private static final String testValidJSON = "{\"mitigation_settings_metadata\":{\"ip_list\":[]}";
    //Generated with: echo -n $ESCAPED_STRING | sha256sum
    private static final String validJSONChecksum = "8537290d6f3f41e06440768da1c6be972c804d6ccc5681c5561144e1a9045711";
    private static final String ipListTemplate = "{\"mitigation_settings_metadata\":{\"ip_list\":[%s]}}";
    
    private static BlackWatchMitigationActionMetadata testBWMetadata;
    private static MitigationActionMetadata testMetadata;
    private static Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator> resourceTypeValidatorMap
        = new HashMap<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator>();
    
    private static final Map<String, Set<String>> recordedResourcesMap = new HashMap<String, Set<String>>();
    private static final MitigationStateSetting setting = new MitigationStateSetting();  
    private static final Map<String, MitigationStateSetting> locationMitigationState = new HashMap<String, MitigationStateSetting> ();
    
    private static MitigationState mitigationState1;
    private static MitigationState mitigationState2;
    
    @Before
    public void cleanTables() {
        mitigationStateDynamoDBHelper.deleteTable();
        mitigationStateDynamoDBHelper.createTableIfNotExist(5L, 5L);
        resourceAllocationStateDDBHelper.deleteTable();
        resourceAllocationStateDDBHelper.createTableIfNotExist(5L, 5L);
    }
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddress, 
                new IPAddressResourceTypeValidator());
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddressList, 
                new IPAddressListResourceTypeValidator());
        
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        mitigationStateDynamoDBHelper = new MitigationStateDynamoDBHelper(dynamoDBClient, realm, domain, 
                5L, 5L, metricsFactory);
        resourceAllocationStateDDBHelper = new ResourceAllocationStateDynamoDBHelper(dynamoDBClient, realm, domain, 
                5L, 5L, metricsFactory);
        resourceAllocationHelper = new ResourceAllocationHelper(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, metricsFactory);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 
                resourceAllocationStateDDBHelper, resourceAllocationHelper, resourceTypeValidatorMap, 4);

        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();
        tsdMetrics = new TSDMetrics(metricsFactory);
        
        recordedResourcesMap.put("ELB", ImmutableSet.of("ELB-18181"));
        recordedResourcesMap.put("EC2", ImmutableSet.of("EC2-55858"));
               
        testMetadata = MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234,5655"))
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
                .resourceId(testResourceId1)
                .resourceType(testResourceType)
                .changeTime(23232L)
                .ownerARN(testOwnerARN)
                .recordedResources(recordedResourcesMap)
                .locationMitigationState(locationMitigationState)
                .state(MitigationState.State.Active.name())
                .ppsRate(1121L)
                .bpsRate(2323L)
                .mitigationSettingsJSON("{ \"IncludeEC2NetworkACLs\":\"True\", \"ip_validation\": \"DROP\"}")
                .mitigationSettingsJSONChecksum("ABABABABA")
                .minutesToLive(100)
                .latestMitigationActionMetadata(testBWMetadata)
                .build();
        
        mitigationState2 = MitigationState.builder()
                .mitigationId(testMitigation2)
                .resourceId(testResourceId2)
                .resourceType(testResourceType)
                .changeTime(23232L)
                .ownerARN(testOwnerARN)
                .recordedResources(recordedResourcesMap)
                .locationMitigationState(locationMitigationState)
                .state(MitigationState.State.Active.name())
                .ppsRate(1121L)
                .bpsRate(2323L)
                .mitigationSettingsJSON("{ \"IncludeEC2NetworkACLs\":\"True\", \"ip_validation\": \"DROP\"}")
                .mitigationSettingsJSONChecksum("ABABABABA")
                .minutesToLive(100)
                .latestMitigationActionMetadata(BlackWatchMitigationActionMetadata.builder()
                        .user("Khaleesi")
                        .toolName("JUnit")
                        .description("Test Descr")
                        .relatedTickets(Arrays.asList("1234,5655"))
                        .build())
                .build();
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
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId(), requestMetadata);
        } catch (ConditionalCheckFailedException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        mitigationState1.setState(MitigationState.State.Active.name());
    }

    @Test
    public void testDeactivateBlackWatchMitigationNonExistant() {
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
        String newOwnerARN  = "new" + testOwnerARN;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.changeOwnerARN(mitigationState1.getMitigationId(), newOwnerARN, testOwnerARN, requestMetadata);
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
        String newOwnerARN  = "new" + testOwnerARN;
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
        String newOwnerARN  = "new" + testOwnerARN;
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
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, testResourceId2, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterByResourceType() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource type, both mitigations match the resource type, return both
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, testResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 2);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
        validateMitigation(listOfBlackwatchMitigation.get(1), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptions() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by mitigation id, resource id, resource type, only one match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, testResourceId1, testResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptionsNoMatch() {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2));  
        //filter by mitigation id, resource id, resource type, no match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, testResourceId2, testResourceType, null, 5, tsdMetrics);
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
                blackWatchMitigationInfoHandler.getHexStringChecksum("Bryan"));
        assertNull(blackWatchMitigationInfoHandler.getHexStringChecksum(null));
    }
    
    @Test
    public void testUpdateBlackWatchMitigationSuccess() {
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1222", tsdMetrics);
        assertNotNull(applyResponse);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);
        
        UpdateBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                applyResponse.getMitigationId(), 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1122", tsdMetrics);
        assertNotNull(response);
    }
    
    @Test
    public void testUpdateBlackWatchMitigationFailedNoMit() {
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                "NotThere", 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1122", tsdMetrics);
    }
    
    @Test
    public void testUpdateBlackWatchMitigationFailedBadState() {
        ApplyBlackWatchMitigationResponse applyResponse = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1222", tsdMetrics);
        assertNotNull(applyResponse);
        assertTrue(applyResponse.isNewMitigationCreated());
        assertTrue(applyResponse.getMitigationId().length() > 0);
        
        MitigationState mitState = mitigationStateDynamoDBHelper.getMitigationState(applyResponse.getMitigationId());
        mitState.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.updateMitigationState(mitState);
        
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.updateBlackWatchMitigation(
                "NotThere", 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1122", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationNewIPAddressSuccess() {
        
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        assertTrue(response.getMitigationId().length() > 0);
        
        ResourceAllocationState ras = resourceAllocationStateDDBHelper.getResourceAllocationState(testIPAddressResourceId);
        assertNotNull(ras);
        assertEquals(response.getMitigationId(), ras.getMitigationId());
        assertEquals(ras.getResourceType(), testIPAddressResourceType);
        assertTrue(ras.getConfirmed());
        
        MitigationState mitState = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(mitState);
        assertEquals(testIPAddressResourceId, mitState.getResourceId());
        assertEquals(testIPAddressResourceType, mitState.getResourceType());
        assertTrue(5L == mitState.getPpsRate());
        assertTrue(5L == mitState.getBpsRate());
        assertTrue(30 == mitState.getMinutesToLive());
        assertEquals(testBWMetadata, mitState.getLatestMitigationActionMetadata());
        assertEquals(testValidJSON, mitState.getMitigationSettingsJSON());
        assertEquals(validJSONChecksum, mitState.getMitigationSettingsJSONChecksum());
        assertEquals("ARN-1222", mitState.getOwnerARN());
        Map<String, Set<String>> rrs = mitState.getRecordedResources();
        assertNotNull(rrs);
        assertEquals(rrs.size(), 1);
        assertEquals(rrs.get(testIPAddressResourceType), ImmutableSet.of(testIPAddressResourceId));
    }
    
    @Test
    public void testApplyBlackWatchMitigationNotNewSuccess() {
        
        ApplyBlackWatchMitigationResponse response1 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        assertNotNull(response1);
        assertTrue(response1.isNewMitigationCreated());
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        
        MitigationState mitState1 = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        assertNotNull(mitState1);
        assertEquals(testIPAddressResourceId, mitState1.getResourceId());
        assertEquals(testIPAddressResourceType, mitState1.getResourceType());
        assertTrue(5L == mitState1.getPpsRate());
        assertTrue(5L == mitState1.getBpsRate());
        assertTrue(30 == mitState1.getMinutesToLive());
        assertEquals(testBWMetadata, mitState1.getLatestMitigationActionMetadata());
        assertEquals(testValidJSON, mitState1.getMitigationSettingsJSON());
        assertEquals(validJSONChecksum, mitState1.getMitigationSettingsJSONChecksum());
        assertEquals("ARN-1222", mitState1.getOwnerARN());
        Map<String, Set<String>> rrs = mitState1.getRecordedResources();
        assertNotNull(rrs);
        assertEquals(rrs.size(), 1);
        assertEquals(rrs.get(testIPAddressResourceType), ImmutableSet.of(testIPAddressResourceId));
        
        ApplyBlackWatchMitigationResponse response2 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 10L, 10L, 30, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        assertNotNull(response2);
        assertFalse(response2.isNewMitigationCreated());
        assertEquals(mitigationId, response2.getMitigationId());
        
        MitigationState mitState2 = mitigationStateDynamoDBHelper.getMitigationState(mitigationId);
        assertTrue(10L == mitState2.getPpsRate());
        assertTrue(10L == mitState2.getBpsRate());
        assertNotEquals(mitState1.getChangeTime(), mitState2.getChangeTime());
        assertNotEquals(mitState1.getVersionNumber(), mitState2.getVersionNumber());
        //Reset the only fields we expect to change from a second Apply call.
        mitState2.setBpsRate(5L);
        mitState2.setPpsRate(5L);
        mitState2.setChangeTime(mitState1.getChangeTime());
        mitState2.setVersionNumber(mitState1.getVersionNumber());
        assertEquals(mitState1, mitState2);
        
    }
    
    @Test
    public void testApplyBlackWatchMitigationDiffARN() {
        
        ApplyBlackWatchMitigationResponse response1 = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        assertNotNull(response1);
        assertTrue(response1.isNewMitigationCreated());
        String mitigationId = response1.getMitigationId();
        assertNotNull(mitigationId);
        
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(testIPAddressResourceId, 
                    testIPAddressResourceType, 5L, 5L, 30, testMetadata, testValidJSON, "ARN-1255", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationBadType() {
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, "BADType", 5L, 5L, 30, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationOverrideMinstoLive() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, null, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertTrue(ms.getMinutesToLive() > 0);
        
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 0, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        assertTrue(ms.getMinutesToLive() > 0);
    }
    
    @Test
    public void testApplyBlackWatchMitigationMissingTableEntries() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        assertNotNull(ms);
        mitigationStateDynamoDBHelper.deleteMitigationState(ms);
        
        thrown.expect(IllegalArgumentException.class);
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationMismatchType() {
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        
        thrown.expect(IllegalArgumentException.class);
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, "MismatchType", 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInactiveFailure() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        
        MitigationState ms = mitigationStateDynamoDBHelper.getMitigationState(response.getMitigationId());
        ms.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.updateMitigationState(ms);
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Could not save MitigationState due to conditional failure!");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                testIPAddressResourceId, testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddress() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3", testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressCIDR() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4/32", testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidV6IPAddress() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("is not a valid IP address!  Resource");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation("2001:0db8:85a3:BBZZ:0000:8a2e:0370:7334", 
                testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidAddresses() {
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "1.2.3.4", testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, 
                "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 
                testIPAddressResourceType, 5L, 5L, 10, testMetadata, testValidJSON, "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListMissing() {
        //Non existent json
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("IP List with one or more entries");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, "{\"BBTest\":\"[]\"}", 
                "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListEmpty() {
        //Blank array
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("IP List with one or more entries");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, ""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListWithCIDR() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid values found in IP List");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate,"\"1.2.3.4/32\""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListBadEntry() {
        //invalid entry
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("1.2.3.Z");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate,"\"1.2.3.Z\""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListBadEntryWithin() {
        //Mixed invalid
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("1.2.5.X");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, "\"1.2.3.4\",\"1.2.5.X\""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListNoDuplicates() {
        //Duplicate addresses specified.
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("duplicate IPs");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, "\"1.2.3.4\",\"5.5.4.4\",\"1.2.3.4\""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationInvalidIPAddressListV6Equivalent() {
        //IPV6 addresses that are different strings, but are equivalent.
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("duplicate IPs");
        blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, "\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\","
                        + "\"2001:0db8:85a3::8a2e:0370:7334\""), "ARN-1222", tsdMetrics);
    }
    
    @Test
    public void testApplyBlackWatchMitigationValidIPAddressList() {
        //Valid
        ApplyBlackWatchMitigationResponse response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, "\"1.2.3.4\""), "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertTrue(response.isNewMitigationCreated());
        
        //Valid V6 + V4.
        response = blackWatchMitigationInfoHandler.applyBlackWatchMitigation(
                "IPList-ABCD", testIPAddressListResourceType, 5L, 5L, 10, testMetadata, 
                String.format(ipListTemplate, "\"2001:0db8:85a3:0000:0000:8a2e:0370:7334\", \"1.2.3.4\""), 
                "ARN-1222", tsdMetrics);
        assertNotNull(response);
        assertFalse(response.isNewMitigationCreated());
    }
}

