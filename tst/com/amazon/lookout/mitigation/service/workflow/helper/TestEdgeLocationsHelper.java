package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

import org.junit.BeforeClass;
import org.junit.Test;

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestEdgeLocationsHelper {
    
    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testHappyCase() {
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
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedAllPOPs = new HashSet<>(edgeServicesPOPs);
        expectedAllPOPs.addAll(daasPOPs);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new NullMetricsFactory());
        Set<String> allPOPs = locationsHelper.getAllPOPs();
        assertEquals(allPOPs, expectedAllPOPs);
        
        Set<String> blackwatchPOPs = locationsHelper.getBlackwatchPOPs();
        assertEquals(blackwatchPOPs, Sets.newHashSet("POP5"));
        
        Set<String> nonBlackwatchPOPs = locationsHelper.getAllNonBlackwatchPOPs();
        expectedAllPOPs.remove("POP5");
        assertEquals(nonBlackwatchPOPs, expectedAllPOPs);
    }
    
    @Test
    public void testCloudFrontCustomerAPIFailureCase() {
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
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new NullMetricsFactory());
        
        Throwable caughtException = null;
        try {
            locationsHelper.getAllPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getAllNonBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    @Test
    public void testRoute53CustomerAPIFailureCase() {
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
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new NullMetricsFactory());
        
        Throwable caughtException = null;
        try {
            locationsHelper.getAllPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getAllNonBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    @Test
    public void testBlackwatchRefreshFailureCase() {
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
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false);
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException());
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new NullMetricsFactory());
        
        Throwable caughtException = null;
        try {
            locationsHelper.getAllPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
        
        caughtException = null;
        try {
            locationsHelper.getAllNonBlackwatchPOPs();
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
    
    @Test
    public void testInitialSuccessButSomeRefreshFailuresLater() {
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
        BlackwatchLocationsHelper bwLocationsHelper = new BlackwatchLocationsHelper(provider, false);
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add("POP5");
        hostclassSearchResult.put("POP5", hosts);
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        when(provider.search("ou=systems,ou=infrastructure,o=amazon.com", "(amznDiscoHostClass=AWS-EDGE-POP5-BW)", Integer.MAX_VALUE, ImmutableList.of("cn"))).thenReturn(result);
        
        Set<String> expectedAllPOPs = new HashSet<>(edgeServicesPOPs);
        expectedAllPOPs.addAll(daasPOPs);
        
        EdgeLocationsHelper locationsHelper = new EdgeLocationsHelper(edgeServicesClient, daasClient, bwLocationsHelper, 1, new NullMetricsFactory());
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
