package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import amazon.odin.awsauth.OdinAWSCredentialsProvider;

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
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new HashSet<String>(), new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchPOPs();
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
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new HashSet<String>(), new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getBlackwatchPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getAllNonBlackwatchPOPs();
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
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new HashSet<String>(), new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllPOPs();
        assertEquals(pops.size(), 3);
        assertEquals(pops, Sets.newHashSet(edgeServicesPOPs));
        
        pops = locationsHelper.getBlackwatchPOPs();
        assertEquals(pops.size(), 1);
        
        pops = locationsHelper.getAllNonBlackwatchPOPs();
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
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new HashSet<String>(), new NullMetricsFactory());
        
        Set<String> pops = locationsHelper.getAllPOPs();
        assertEquals(pops.size(), 3);
        
        pops = locationsHelper.getBlackwatchPOPs();
        assertEquals(pops.size(), 0);
        
        pops = locationsHelper.getAllNonBlackwatchPOPs();
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
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new HashSet<String>(), new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchPOPs();
        Set<String> expectedNonBlackwatchPOPs = new HashSet<String>(expectedAllPOPs);
        expectedNonBlackwatchPOPs.remove("POP5");
        assertEquals(nonBlackwatchPOPs, expectedNonBlackwatchPOPs);
        
        // Force refresh.
        locationsHelper.run();
        allPOPs = locationsHelper.getAllPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        verify(edgeServicesClient, times(2)).newGetPOPsCall();
    }
}
