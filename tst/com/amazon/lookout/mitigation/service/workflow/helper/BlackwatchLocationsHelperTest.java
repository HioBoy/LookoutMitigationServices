package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import amazon.mws.data.Datapoint;
import amazon.mws.data.StatisticSeries;
import amazon.mws.query.MonitoringQueryClient;
import amazon.mws.request.MWSRequest;
import amazon.mws.response.Error;
import amazon.mws.response.GetMetricDataResponse;
import amazon.mws.response.ResponseException;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.ldaputils.LdapProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class BlackwatchLocationsHelperTest {
    private static final String BLACKWATCH_POP = "BlackwatchPOP";
    private static final String NON_BLACKWATCH_POP = "NonBlackwatchPOP";

    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    public static class MockMonitoringQueryClientProvider extends MonitoringQueryClientProvider {
        private final MonitoringQueryClient mockMonitoringQueryClient;
        
        public MockMonitoringQueryClientProvider(OdinAWSCredentialsProvider credsProvider, MonitoringQueryClient mockMonitoringQueryClient) {
            super(credsProvider, "us-east-1");
            this.mockMonitoringQueryClient = mockMonitoringQueryClient;
        }
        
        @Override
        public MonitoringQueryClient getClient() {
            return mockMonitoringQueryClient;
        }
    }
    
    /**
     * Test to verify we return true for a POP which has BW hosts are built + it also has recent data above threshold for BW metric in MWS.
     * @throws Exception
     */
    @Test
    public void testForBlackwatchPOP() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        amazon.query.types.DateTime queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(3).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(2).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        response.setNumberOfAvailable(2);
        response.setNumberOfReturned(2);
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertTrue(isBlackwatchPOP);
    }
    
    /**
     * Test to verify we return false for a POP which has no BW hosts built.
     * @throws Exception
     */
    @Test
    public void testForNonBlackwatchPOPWithNoHostsBuild() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(NON_BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertFalse(isBlackwatchPOP);
    }
    
    /**
     * Test to verify we return false for a POP which has BW hosts built, but has no recent data for BW metric in MWS.
     * @throws Exception
     */
    @Test
    public void testForPOPWithNoRecentMWSData() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(NON_BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertFalse(isBlackwatchPOP);
    }
    
    /**
     * Test the creation of BW host class name for a given POP.
     * @throws Exception
     */
    @Test
    public void testBWHostclassNameForPOP() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        String bwHostclass = helper.createBWHostclassForPOP(BLACKWATCH_POP);
        
        assertEquals(bwHostclass, "AWS-EDGE-" + BLACKWATCH_POP.toUpperCase() + "-BW");
    }
    
    /**
     * Test we retry on transient LDAP query failures.
     * @throws Exception
     */
    @Test
    public void testRetriesOnLDAPQueryFailure() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        amazon.query.types.DateTime queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(3).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(2).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        response.setNumberOfAvailable(2);
        response.setNumberOfReturned(2);
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenThrow(new RuntimeException()).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertTrue(isBlackwatchPOP);
        verify(provider, times(3)).search(anyString(), anyString(), anyInt(), anyList());
    }
    
    @Test
    public void testRetriesOnMWSQueryFailure() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        DateTime now = new DateTime(DateTimeZone.UTC);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        amazon.query.types.DateTime queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(3).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(2).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        response.setNumberOfAvailable(2);
        response.setNumberOfReturned(2);
        response.addStatisticSeries(series);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenThrow(new RuntimeException()).thenReturn(response);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, true, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertTrue(isBlackwatchPOP);
        verify(provider, times(1)).search(anyString(), anyString(), anyInt(), anyList());
        verify(mockMonitoringQueryClient, times(2)).requestResponse(any(MWSRequest.class));
    }
    
    @Test
    public void testWhenMWSQueryThrowsNoMetricFoundException() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        DateTime now = new DateTime(DateTimeZone.UTC);
        
        GetMetricDataResponse response = new GetMetricDataResponse();
        StatisticSeries series = new StatisticSeries();
        response.addStatisticSeries(series);
        
        amazon.query.types.DateTime queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(3).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        queryDateTime = new amazon.query.types.DateTime(now.minusMinutes(2).getMillis());
        series.addDatapoint(new Datapoint(queryDateTime, 60.0));
        
        response.setNumberOfAvailable(2);
        response.setNumberOfReturned(2);
        response.addStatisticSeries(series);
        
        HttpURLConnection requestConn = mock(HttpURLConnection.class);
        Error error = new Error();
        error.setCode("MetricNotFound");
        error.setMessage("No metrics matched your request parameters");
        response.addError(error);
        ResponseException exception = new ResponseException(requestConn, response);
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenThrow(exception);
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, true, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        assertFalse(isBlackwatchPOP);
        verify(provider, times(1)).search(anyString(), anyString(), anyInt(), anyList());
        verify(mockMonitoringQueryClient, times(1)).requestResponse(any(MWSRequest.class));
    }
    
    /**
     * Test to ensure we throw an exception when we fail to query MWS for BW metric data.
     * @throws Exception
     */
    @Test
    public void testThrowExceptionOnFailure() throws Exception {
        LdapProvider provider = mock(LdapProvider.class);
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException());
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenThrow(new RuntimeException());
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        Throwable caughtException = null;
        try {
            helper.isBlackwatchPOP(BLACKWATCH_POP, TestUtils.newNopTsdMetrics());
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        verify(provider, times(3)).search(anyString(), anyString(), anyInt(), anyList());
        verify(mockMonitoringQueryClient, times(3)).requestResponse(any(MWSRequest.class));
    }
}
