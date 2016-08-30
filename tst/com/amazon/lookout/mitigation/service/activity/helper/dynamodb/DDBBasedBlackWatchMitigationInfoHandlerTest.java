
package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.LocationMitigationStateSettings;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazon.blackwatch.mitigation.state.model.MitigationActionMetadata;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.blackwatch.mitigation.state.model.MitigationState.Setting;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

public class DDBBasedBlackWatchMitigationInfoHandlerTest {
    private static final String domain = "unit-test-domain";
    private static final String realm = "unit-test-realm";
    
    private static AmazonDynamoDBClient dynamoDBClient;
    private static DDBBasedBlackWatchMitigationInfoHandler blackWatchMitigationInfoHandler;
    private static MitigationStateDynamoDBHelper mitigationStateDynamoDBHelper;

    private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    private static Metrics metrics = Mockito.mock(Metrics.class);
    private static TSDMetrics tsdMetrics;

    
    private static final String testMitigation1 = "testMitigation-20160818";
    private static final String testMitigation2 = "testMitigation-20160819";
    private static final String testResourceId2 = "TEST-RESOURCE-2";

    private static final String testOwnerARN = "testOwnerARN";
    private static final String testResourceId1 = "TEST-RESOURCE-1";
    private static final String testLocation = "BR-SFO5-1";
    
    private static final String testResourceType = "testResourceType";

    
    private static final Map<String, List<String>> recordedResourcesMap = new HashMap<String, List<String>>();
    private static final Setting setting = new Setting();  
    private static final Map<String, Setting> locationMitigationState = new HashMap<String, Setting> ();

    private static MitigationState mitigationState1;
    private static MitigationState mitigationState2;
    
    @Before
    public void cleanTable() {
        mitigationStateDynamoDBHelper.deleteTable();
        mitigationStateDynamoDBHelper.createTableIfNotExist(5L, 5L);
    }
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        mitigationStateDynamoDBHelper = new MitigationStateDynamoDBHelper(dynamoDBClient, realm, domain, 51, 51, metricsFactory);
        blackWatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDynamoDBHelper, 4);

        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();
        tsdMetrics = new TSDMetrics(metricsFactory);
        
        recordedResourcesMap.put("ELB", Arrays.asList("ELB-18181"));
        recordedResourcesMap.put("EC2", Arrays.asList("EC2-55858"));

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
                .latestMitigationActionMetadata(MitigationActionMetadata.builder()
                        .user("Khaleesi")
                        .toolName("JUnit")
                        .description("Test Descr")
                        .relatedTickets(Arrays.asList("1234,5655"))
                        .build())
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
                .latestMitigationActionMetadata(MitigationActionMetadata.builder()
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
        Map<String, Setting> locationMitigationStateSettingsExpected = ms.getLocationMitigationState();
        assertEquals(locationMitigationStateSettings.keySet(), locationMitigationStateSettingsExpected.keySet());
        for (String key : locationMitigationStateSettings.keySet()) {
            assertEquals(locationMitigationStateSettings.get(key).getBPS(), locationMitigationStateSettingsExpected.get(key).getBPS());
            assertEquals(locationMitigationStateSettings.get(key).getPPS(), locationMitigationStateSettingsExpected.get(key).getPPS());
            assertEquals(locationMitigationStateSettings.get(key).getMitigationSettingsJSONChecksum(), locationMitigationStateSettingsExpected.get(key).getMitigationSettingsJSONChecksum());
        }

    }

    @Test
    public void testDeactivateBlackWatchMitigation() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId());
        MitigationState newMitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationState1.getMitigationId());
        assertEquals(MitigationState.State.To_Delete.name(), newMitigationState.getState());
    }

    @Test
    public void testDeactivateBlackWatchMitigationFail() throws IOException {
        mitigationState1.setState(MitigationState.State.To_Delete.name());
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId());
        } catch (ConditionalCheckFailedException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        mitigationState1.setState(MitigationState.State.Active.name());
    }

    @Test
    public void testDeactivateBlackWatchMitigationNonExistant() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.deactivateMitigation(mitigationState1.getMitigationId() + "Fail");
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        mitigationState1.setState(MitigationState.State.Active.name());
    }

    @Test
    public void testChangeOwnerARN() throws IOException {
        String newOwnerARN  = "new" + testOwnerARN;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        blackWatchMitigationInfoHandler.changeOwnerARN(mitigationState1.getMitigationId(), newOwnerARN, testOwnerARN);
        MitigationState newMitigationState = mitigationStateDynamoDBHelper.getMitigationState(mitigationState1.getMitigationId());
        assertEquals(newOwnerARN, newMitigationState.getOwnerARN());
    }

    @Test
    public void testChangeOwnerARNFail() throws IOException {
        String newOwnerARN  = "new" + testOwnerARN;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.changeOwnerARN(mitigationState1.getMitigationId(), newOwnerARN, newOwnerARN);
        } catch (ConditionalCheckFailedException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testChangeOwnerARNFailNonExistant() throws IOException {
        String newOwnerARN  = "new" + testOwnerARN;
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        Throwable caughtException = null;
        try {
            blackWatchMitigationInfoHandler.changeOwnerARN("Fail" + mitigationState1.getMitigationId(), newOwnerARN, newOwnerARN);
        } catch (IllegalArgumentException ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }

    @Test
    public void testGetBlackWatchMitigationsNoFilter() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 

        //no filter, should return all mitigations
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 2);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
        validateMitigation(listOfBlackwatchMitigation.get(1), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByMitigationId() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        
        //filter by mitigation id
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, null, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterByResourceId() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource id
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, testResourceId2, null, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFiterByResourceType() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by resource type, both mitigations match the resource type, return both
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, testResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 2);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
        validateMitigation(listOfBlackwatchMitigation.get(1), mitigationState2);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptions() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //filter by mitigation id, resource id, resource type, only one match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, testResourceId1, testResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByAllOptionsNoMatch() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2));  
        //filter by mitigation id, resource id, resource type, no match
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(testMitigation1, testResourceId2, testResourceType, null, 5, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 0);
    }
    
    @Test
    public void testGetBlackWatchMitigationsFilterByMaxEntry() throws IOException {
        mitigationStateDynamoDBHelper.batchUpdateState(Arrays.asList(mitigationState1, mitigationState2)); 
        //set max # of entries to return to be 1, we should only return 1 mitigaiton. 
        List<BlackWatchMitigationDefinition> listOfBlackwatchMitigation = blackWatchMitigationInfoHandler.getBlackWatchMitigations(null, null, null, null, 1, tsdMetrics);
        assertEquals(listOfBlackwatchMitigation.size(), 1);
        validateMitigation(listOfBlackwatchMitigation.get(0), mitigationState1);
    }
}

