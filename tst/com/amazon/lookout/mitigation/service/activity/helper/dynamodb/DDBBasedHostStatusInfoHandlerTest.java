
package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;
import com.amazon.lookout.test.common.dynamodb.DynamoDBTestUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazon.blackwatch.host.status.model.HostStatus;
import com.amazon.blackwatch.host.status.model.HostType;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.host.status.storage.HostStatusDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

import static com.amazon.blackwatch.host.status.model.HostConfigApplyStatus.*;

public class DDBBasedHostStatusInfoHandlerTest {
    private static final String domain = "unit-test";
    private static final String realm = "unit-test";
    private static final String deviceName = "testDeviceName";
    private static final String hardwareType = "testHardwareType";

    private static AmazonDynamoDBClient dynamoDBClient;
    private static DDBBasedHostStatusInfoHandler hostStatusInfoHandler;
    private static HostStatusDynamoDBHelper hostStatusDynamoDBHelper;

    private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    private static Metrics metrics = Mockito.mock(Metrics.class);
    private static TSDMetrics tsdMetrics;

    private static final String HOST_TYPE = HostType.BLACKWATCH.name();
    private static final String STATUS_DETAILS = "{\"CPU speed\":12345}";
    private static final String STATUS_DESCRIPTION = "config 123455 is rejected by blackwatch process";

    private final String location1 = "location 1";
    private final String host1 = "host";
    private final String configId1 = "test config 1";
    private final String location2 = "location 2";

    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
        dynamoDBClient = DynamoDBTestUtil.get().getClient();
        hostStatusInfoHandler = new DDBBasedHostStatusInfoHandler(dynamoDBClient, domain, realm);
        hostStatusDynamoDBHelper = new HostStatusDynamoDBHelper(dynamoDBClient, realm, domain, 51, 51, metricsFactory);

        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();
        tsdMetrics = new TSDMetrics(metricsFactory);
    }

    /**
     * Test all host statuses are retrieved for given location.
     * @throws IOException 
     */
    @Test
    public void testGetLocationHostStatus() throws IOException {
        long heartBeatCount = 10;
        long heartBeatTimestamp = System.currentTimeMillis();
        HostStatus hostStatus;
        try {
            int recordCount = 10;
            
            List<HostStatus> listOfHostStatus = new ArrayList<HostStatus>();
            for(int i=0; i<recordCount; i++) {
                hostStatus = HostStatus.builder()
                    .location(location1)
                    .hostname(host1 + i)
                    .deviceName(deviceName)
                    .hardwareType(hardwareType)
                    .latestHeartBeatCount(heartBeatCount)
                    .latestHeartBeatTimestamp(heartBeatTimestamp)
                    .runningConfig(configId1)
                    .agentReceivedConfig(configId1)
                    .applyNewConfigStatus(ACCEPT.name())
                    .isActive(new Random().nextBoolean())
                    .currentStatus(HostStatusEnum.ACTIVE)
                    .hostType(HOST_TYPE)
                    .statusDescription(STATUS_DESCRIPTION)
                    .build();

                hostStatus.setStatusDetailsWithJSON(STATUS_DETAILS);
                listOfHostStatus.add(hostStatus);
            }
            //add a record with different location name
            hostStatus = HostStatus.builder()
                .location(location2)
                .hostname(host1)
                .deviceName(deviceName)
                .hardwareType(hardwareType)
                .latestHeartBeatCount(heartBeatCount)
                .latestHeartBeatTimestamp(heartBeatTimestamp)
                .runningConfig(configId1)
                .agentReceivedConfig(configId1)
                .applyNewConfigStatus(ACCEPT.name())
                .isActive(new Random().nextBoolean())
                .currentStatus(HostStatusEnum.ACTIVE)
                .hostType(HOST_TYPE)
                .statusDescription(STATUS_DESCRIPTION)
                .build();

            hostStatus.setStatusDetailsWithJSON(STATUS_DETAILS);
            listOfHostStatus.add(hostStatus);

            for(int i=0; i<listOfHostStatus.size(); i++) {
                hostStatusDynamoDBHelper.updateHostStatus(listOfHostStatus.get(i), tsdMetrics);
            }

            // validate all host status of a location are retrieved.
            List<HostStatusInLocation> hostStatuses = hostStatusInfoHandler.getHostsStatus(location1, tsdMetrics);
            assertEquals(recordCount, hostStatuses.size());

            for (int i = 0; i < recordCount; ++i) {
                assertEquals(i, Integer.parseInt(hostStatuses.get(i).getHostName().split(host1)[1]));
            }

            hostStatuses = hostStatusInfoHandler.getHostsStatus(location2, tsdMetrics);
            assertEquals(1, hostStatuses.size());
        }
        finally {
            hostStatusDynamoDBHelper.deleteAllItemsFromTable();
        }
    }

    /**
     * Test all host statuses are retrieved for given location and they are correct.
     * @throws IOException 
     */
    @Test
    public void testGetLocationHostStatusVerifyContent() throws IOException {
        long heartBeatCount = 10;
        long heartBeatTimestamp = System.currentTimeMillis();
        HostStatus hostStatus;
        try {
            int recordCount = 10;
            
            List<HostStatus> listOfHostStatus = new ArrayList<HostStatus>();
            for(int i=0; i<recordCount; i++) {
                hostStatus = HostStatus.builder()
                    .location(location1)
                    .hostname(host1 + i)
                    .deviceName(deviceName)
                    .hardwareType(hardwareType)
                    .latestHeartBeatCount(heartBeatCount)
                    .latestHeartBeatTimestamp(heartBeatTimestamp)
                    .runningConfig(configId1)
                    .agentReceivedConfig(configId1)
                    .applyNewConfigStatus(ACCEPT.name())
                    .isActive(new Random().nextBoolean())
                    .currentStatus(HostStatusEnum.ACTIVE)
                    .hostType(HOST_TYPE)
                    .statusDescription(STATUS_DESCRIPTION)
                    .build();

                hostStatus.setStatusDetailsWithJSON(STATUS_DETAILS);
                listOfHostStatus.add(hostStatus);
            }

            // insert records in host status table for a location in ddb table
            for(int i=0; i<listOfHostStatus.size(); i++) {
                hostStatusDynamoDBHelper.updateHostStatus(listOfHostStatus.get(i), tsdMetrics);
            }

            // validate all host status of a location are retrieved.
            List<HostStatusInLocation> hostStatuses = hostStatusInfoHandler.getHostsStatus(location1, tsdMetrics);
            assertEquals(listOfHostStatus.size(), hostStatuses.size());

            for (int i = 0; i < recordCount; ++i) {
                // check if hostname matches
                assertEquals(listOfHostStatus.get(i).getHostname(), hostStatuses.get(i).getHostName());
                // check if isActive matches
                assertEquals(listOfHostStatus.get(i).getIsActive(), hostStatuses.get(i).isIsActive());
            }
        }
        finally {
            hostStatusDynamoDBHelper.deleteAllItemsFromTable();
        }
    }

    /**
     * Test location does not have any host statuses
     */
    @Test
    public void testGetLocationHostStatusAtNonExistingLocation() {
        assertEquals(0, hostStatusInfoHandler.getHostsStatus("random location1!", tsdMetrics).size());
    }
}

