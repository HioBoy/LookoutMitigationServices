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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;

import amazon.mws.data.Datapoint;
import amazon.mws.data.StatisticSeries;
import amazon.mws.query.MonitoringQueryClient;
import amazon.mws.request.MWSRequest;
import amazon.mws.response.GetMetricDataResponse;
import amazon.mws.response.ResponseException;
import amazon.odin.awsauth.OdinAWSCredentialsProvider;

import com.amazon.aws158.commons.io.FileUtils;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.daas.control.DNSServer;
import com.amazon.daas.control.DaasControlAPIServiceV20100701Client;
import com.amazon.daas.control.ListDNSServersRequest;
import com.amazon.daas.control.ListDNSServersResponse;
import com.amazon.daas.control.impl.ListDNSServersCall;
import com.amazon.edge.service.EdgeOperatorServiceClient;
import com.amazon.edge.service.GetPOPsResult;
import com.amazon.edge.service.impl.GetPOPsCall;
import com.amazon.ldaputils.LdapProvider;
import com.amazon.lookout.mitigation.service.workflow.helper.BlackwatchLocationsHelperTest.MockMonitoringQueryClientProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EdgeLocationsHelperTest {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    private MonitoringQueryClientProvider getMonitoringQueryClientProviderForBWPOP() throws Exception {
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
        return monitoringQueryClientProvider;
    }
    
    @Test
    public void testHappyCase() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1", "POP3", "POP4", "POP5");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        DNSServer serverPOP3 = new DNSServer();
        serverPOP3.setPOP(daasPOPs.get(1));
        DNSServer serverPOP4 = new DNSServer();
        serverPOP4.setPOP(daasPOPs.get(2));
        DNSServer serverPOP5 = new DNSServer();
        serverPOP5.setPOP(daasPOPs.get(3));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1, serverPOP3, serverPOP4, serverPOP5));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedAllPOPs = new HashSet<>(edgeServicesPOPs);
        expectedAllPOPs.addAll(daasPOPs);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllClassicPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchClassicPOPs();
        expectedAllPOPs.remove("POP5");
        assertEquals(nonBlackwatchPOPs, expectedAllPOPs);
    }
    
    @Test
    public void testCloudFrontCustomerAPIFailureCase() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        when(edgeServicesGetPOPsCall.call()).thenThrow(new RuntimeException());
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllClassicPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getAllNonBlackwatchClassicPOPs();
        assertEquals(pops.size(), 0);
    }
    
    @Test
    public void testRoute53CustomerAPIFailureCase() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllClassicPOPs();
        assertEquals(pops.size(), 3);
        assertEquals(pops, Sets.newHashSet(edgeServicesPOPs));
        
        pops = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(pops.size(), 1);
        
        pops = locationsHelper.getAllNonBlackwatchClassicPOPs();
        assertEquals(pops.size(), 2);
    }
    
    @Test
    public void testBlackwatchRefreshFailureCase() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenThrow(new RuntimeException());
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException());
        
        MonitoringQueryClient mockMonitoringQueryClient = mock(MonitoringQueryClient.class);
        
        when(mockMonitoringQueryClient.requestResponse(any(MWSRequest.class))).thenThrow(new RuntimeException());
        
        OdinAWSCredentialsProvider odinCredsProvider = mock(OdinAWSCredentialsProvider.class);
        when(odinCredsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("abc", "def"));
        
        MonitoringQueryClientProvider monitoringQueryClientProvider = new MockMonitoringQueryClientProvider(odinCredsProvider, mockMonitoringQueryClient);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, monitoringQueryClientProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllClassicPOPs();
        assertEquals(pops.size(), 3);
        
        pops = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getAllNonBlackwatchClassicPOPs();
        assertEquals(pops.size(), 3);
    }
    
    @Test
    public void testInitialSuccessButSomeRefreshFailuresLater() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenThrow(new RuntimeException());
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1", "POP3", "POP4", "POP5");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        DNSServer serverPOP3 = new DNSServer();
        serverPOP3.setPOP(daasPOPs.get(1));
        DNSServer serverPOP4 = new DNSServer();
        serverPOP4.setPOP(daasPOPs.get(2));
        DNSServer serverPOP5 = new DNSServer();
        serverPOP5.setPOP(daasPOPs.get(3));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1, serverPOP3, serverPOP4, serverPOP5));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedAllPOPs = new HashSet<>(edgeServicesPOPs);
        expectedAllPOPs.addAll(daasPOPs);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllClassicPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchClassicPOPs();
        Set<String> expectedNonBlackwatchPOPs = new HashSet<String>(expectedAllPOPs);
        expectedNonBlackwatchPOPs.remove("POP5");
        assertEquals(nonBlackwatchPOPs, expectedNonBlackwatchPOPs);
        
        // Force refresh.
        locationsHelper.run();
        allPOPs = locationsHelper.getAllClassicPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        verify(edgeServicesClient, times(2)).newGetPOPsCall();
    }
    
    @Test
    public void testCaseWhereABWPOPBecomesNonBW() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1", "POP3", "POP4", "POP5");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        DNSServer serverPOP3 = new DNSServer();
        serverPOP3.setPOP(daasPOPs.get(1));
        DNSServer serverPOP4 = new DNSServer();
        serverPOP4.setPOP(daasPOPs.get(2));
        DNSServer serverPOP5 = new DNSServer();
        serverPOP5.setPOP(daasPOPs.get(3));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1, serverPOP3, serverPOP4, serverPOP5));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        MonitoringQueryClientProvider mockProvider = mock(MonitoringQueryClientProvider.class);
        MonitoringQueryClient mockClient = mock(MonitoringQueryClient.class);
        when(mockProvider.getClient()).thenReturn(mockClient);
        
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
        
        ResponseException responseException = mock(ResponseException.class);
        when(responseException.getMessage()).thenReturn("Some message, followed by: MetricNotFound: No metrics matched your request parameters. Followed by some other stuff.");
        when(mockClient.requestResponse(any(MWSRequest.class))).thenReturn(response).thenThrow(responseException);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, mockProvider, "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedAllPOPs = new HashSet<>(edgeServicesPOPs);
        expectedAllPOPs.addAll(daasPOPs);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllClassicPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchClassicPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchClassicPOPs();
        expectedAllPOPs.remove("POP5");
        assertEquals(nonBlackwatchPOPs, expectedAllPOPs);
        
        // On refresh, the pop POP5 should now get marked as a non-BW POP
        locationsHelper.run();
        blackwatchPOPs = locationsHelper.getBlackwatchClassicPOPs();
        assertTrue(blackwatchPOPs.isEmpty());
        
        nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchClassicPOPs();
        assertTrue(nonBlackwatchPOPs.containsAll(Lists.newArrayList("POP1", "POP2", "POP3", "POP4", "POP5")));
    }
    
    @Test
    public void testFilteringOutMetroCFPOPs() throws Exception {
        LdapProvider ldapProvider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(ldapProvider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "POP2", "POP5", "SFO5", "SFO5-M1", "SFO50-M3", "SFO20-M2", "LHR51-M2");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, "dir", new NullMetricsFactory());
        Set<String> cfClassicPOPs = locationsHelper.getCloudFrontClassicPOPs();
        assertNotNull(cfClassicPOPs);
        assertEquals(Sets.newHashSet("POP1", "POP2", "POP5", "SFO5"), cfClassicPOPs);
    }
    
    @Test
    public void testLoadPOPsFromDiskOnStartupWithUnsuccessfulCFPopsRefresh() throws Exception {
        LdapProvider ldapProvider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(ldapProvider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        when(edgeServicesClient.newGetPOPsCall()).thenThrow(new RuntimeException());
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);

        // POPs from file are: "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50"
        Set<String> expectedPOPs = Sets.newHashSet("POP1", "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50");
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, TestUtils.getTstDataPath(EdgeLocationsHelper.class), new NullMetricsFactory());
        Set<String> allClassicPOPs = locationsHelper.getAllClassicPOPs();
        assertNotNull(allClassicPOPs);
        assertEquals(expectedPOPs, allClassicPOPs);
    }
    
    @Test
    public void testLoadPOPsFromDiskOnStartupWithUnsuccessfulRoute53PopsRefresh() throws Exception {
        LdapProvider ldapProvider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(ldapProvider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "ARN1");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        when(daasClient.newListDNSServersCall()).thenThrow(new RuntimeException());

        // POPs from file are: "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50"
        Set<String> expectedPOPs = Sets.newHashSet("POP1", "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50");
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, TestUtils.getTstDataPath(EdgeLocationsHelper.class), new NullMetricsFactory());
        Set<String> allClassicPOPs = locationsHelper.getAllClassicPOPs();
        assertNotNull(allClassicPOPs);
        assertEquals(expectedPOPs, allClassicPOPs);
    }
    
    @Test
    public void testLoadPOPsFromDiskOnStartupWithUnsuccessfulRoute53AndCFPopsRefresh() throws Exception {
        LdapProvider ldapProvider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(ldapProvider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        when(edgeServicesClient.newGetPOPsCall()).thenThrow(new RuntimeException());
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        when(daasClient.newListDNSServersCall()).thenThrow(new RuntimeException());

        // POPs from file are: "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50"
        Set<String> expectedPOPs = Sets.newHashSet("ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50");
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, TestUtils.getTstDataPath(EdgeLocationsHelper.class), new NullMetricsFactory());
        Set<String> allClassicPOPs = locationsHelper.getAllClassicPOPs();
        assertNotNull(allClassicPOPs);
        assertEquals(expectedPOPs, allClassicPOPs);
    }
    
    @Test
    public void testLoadPOPsFromDiskOnStartupWithSuccessfulRefresh() throws Exception {
        LdapProvider ldapProvider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(ldapProvider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1", "ARN1", "BOM2", "SFO5", "GIG50", "MAA3");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1", "BOM2", "SEA19");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        DNSServer serverPOP2 = new DNSServer();
        serverPOP2.setPOP(daasPOPs.get(1));
        DNSServer serverPOP3 = new DNSServer();
        serverPOP3.setPOP(daasPOPs.get(2));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1, serverPOP2, serverPOP3));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);

        // POPs from file are: "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19", "MIA50", however, since MIA50 isn't returned by 
        // either of the EdgeOperatorService/DaasControlService above, we don't expect it in the final result set.
        Set<String> expectedPOPs = Sets.newHashSet("POP1", "ARN1", "BOM2", "SFO5", "GIG50", "MAA3", "SEA19");
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, TestUtils.getTstDataPath(EdgeLocationsHelper.class), new NullMetricsFactory());
        Set<String> allClassicPOPs = locationsHelper.getAllClassicPOPs();
        assertNotNull(allClassicPOPs);
        assertEquals(expectedPOPs, allClassicPOPs);
    }
    
    @Test
    public void testFlushPOPsFromDisk() throws Exception {
        EdgeOperatorServiceClient edgeServicesClient = mock(EdgeOperatorServiceClient.class);
        GetPOPsCall edgeServicesGetPOPsCall = mock(GetPOPsCall.class);
        GetPOPsResult edgeServicesGetPOPsResult = new GetPOPsResult();
        List<String> edgeServicesPOPs = Lists.newArrayList("POP1");
        edgeServicesGetPOPsResult.setPOPList(edgeServicesPOPs);
        when(edgeServicesGetPOPsCall.call()).thenReturn(edgeServicesGetPOPsResult);
        when(edgeServicesClient.newGetPOPsCall()).thenReturn(edgeServicesGetPOPsCall);
        
        DaasControlAPIServiceV20100701Client daasClient = mock(DaasControlAPIServiceV20100701Client.class);
        ListDNSServersCall dnsServersCall = mock(ListDNSServersCall.class);
        ListDNSServersResponse dnsServersResponse = new ListDNSServersResponse();
        List<String> daasPOPs = Lists.newArrayList("POP1", "POP5");
        DNSServer serverPOP1 = new DNSServer();
        serverPOP1.setPOP(daasPOPs.get(0));
        DNSServer serverPOP2 = new DNSServer();
        serverPOP2.setPOP(daasPOPs.get(1));
        dnsServersResponse.setResults(Lists.newArrayList(serverPOP1, serverPOP2));
        when(dnsServersCall.call(any(ListDNSServersRequest.class))).thenReturn(dnsServersResponse);
        when(daasClient.newListDNSServersCall()).thenReturn(dnsServersCall);
        
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false, getMonitoringQueryClientProviderForBWPOP(), "Prod", "Total_Mitigated_Packets_RX", 5);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedPOPs = Sets.newHashSet("POP1", "POP5");
        
        String tempDir = TestUtils.getTempDataPath(EdgeLocationsHelper.class);
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, tempDir, new NullMetricsFactory());
        Set<String> allClassicPOPs = locationsHelper.getAllClassicPOPs();
        assertEquals(expectedPOPs, allClassicPOPs);
        
        locationsHelper.flushCurrentListOfPOPsToDisk();
        
        File flushedFile = new File(tempDir, EdgeLocationsHelper.POPS_LIST_FILE_NAME);
        assertTrue(flushedFile.exists());
        
        List<List<String>> lines = FileUtils.readFile(flushedFile.getAbsolutePath(), EdgeLocationsHelper.POPS_LIST_FILE_FIELDS_DELIMITER, 
                                                      EdgeLocationsHelper.POPS_LIST_FILE_COMMENTS_KEY, EdgeLocationsHelper.POPS_LIST_FILE_CHARSET);
        assertNotNull(lines);
        
        assertEquals(lines.size(), 2);
        
        List<String> line1 = lines.get(0);
        List<String> line2 = lines.get(1);
        assertTrue((line1.get(0).equals("POP1") && line2.get(0).equals("POP5")) || (line1.get(0).equals("POP5") && line2.get(0).equals("POP1")));
        boolean isPOP1BW = (line1.get(0).equals("POP1") ? Boolean.valueOf(line1.get(1)) : Boolean.valueOf(line2.get(1)));
        boolean isPOP5BW = (line1.get(0).equals("POP5") ? Boolean.valueOf(line1.get(1)) : Boolean.valueOf(line2.get(1)));
        assertFalse(isPOP1BW);
        assertTrue(isPOP5BW);
    }
}
