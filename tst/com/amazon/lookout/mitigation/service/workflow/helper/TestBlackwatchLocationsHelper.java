package com.amazon.lookout.mitigation.service.workflow.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.ldaputils.LdapProvider;

public class TestBlackwatchLocationsHelper {
    private static final String BLACKWATCH_POP = "BlackwatchPOP";
    private static final String NON_BLACKWATCH_POP = "NonBlackwatchPOP";

    @BeforeClass
    public static void setup() {
        TestUtils.configure();
    }
    
    @Test
    public void testForBlackwatchPOP() {
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP);
        assertTrue(isBlackwatchPOP);
    }
    
    @Test
    public void testForNonBlackwatchPOP() {
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(NON_BLACKWATCH_POP);
        assertFalse(isBlackwatchPOP);
    }
    
    @Test
    public void testBWHostclassNameForPOP() {
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false);
        
        String bwHostclass = helper.createBWHostclassForPOP(BLACKWATCH_POP);
        
        assertEquals(bwHostclass, "AWS-EDGE-" + BLACKWATCH_POP.toUpperCase() + "-BW");
    }
    
    @Test
    public void testRetriesOnFailure() {
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException()).thenThrow(new RuntimeException()).thenReturn(result);
        boolean isBlackwatchPOP = helper.isBlackwatchPOP(BLACKWATCH_POP);
        assertTrue(isBlackwatchPOP);
        verify(provider, times(3)).search(anyString(), anyString(), anyInt(), anyList());
    }
    
    @Test
    public void testThrowExceptionOnFailure() {
        LdapProvider provider = mock(LdapProvider.class);
        BlackwatchLocationsHelper helper = new BlackwatchLocationsHelper(provider, false);
        
        Map<String, List<Object>> hostclassSearchResult = new HashMap<>();
        List<Object> hosts = new ArrayList<>();
        hosts.add(BLACKWATCH_POP);
        hostclassSearchResult.put(BLACKWATCH_POP, hosts);
        
        List<Map<String, List<Object>>> result = new ArrayList<>();
        result.add(hostclassSearchResult);
        
        when(provider.search(anyString(), anyString(), anyInt(), anyList())).thenThrow(new RuntimeException());
        
        Throwable caughtException = null;
        try {
            helper.isBlackwatchPOP(BLACKWATCH_POP);
        } catch (Exception ex) {
            caughtException = ex;
        }
        assertNotNull(caughtException);
    }
}
