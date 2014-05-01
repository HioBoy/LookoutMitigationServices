package com.amazon.lookout.mitigation.service.helpers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.coral.service.Identity;
import com.google.common.collect.Lists;

import com.amazon.lookout.mitigation.service.constants.DeviceName;

public class AWSUserGroupBasedAuthorizerTest {
    
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure();
    }
    
    private AWSUserGroupBasedAuthorizer authorizer;
    
    @Before
    public void setUpAuthorizer() {
        final String client = AWSUserGroupBasedAuthorizer.CLIENT_SUFFIX;
        final String readOnlyClient = AWSUserGroupBasedAuthorizer.READ_ONLY_CLIENT_SUFFIX;

        final String edgeServicesPopRouterClient = "EdgeServicesPopRouter" + client;
        final String edgeServicesPopRouterReadOnlyClient = "EdgeServicesPopRouter" + readOnlyClient;
        final String lookoutMitigationServiceExplorer = AWSUserGroupBasedAuthorizer.LOOKOUT_MITIGATION_SERVICE_EXPLORER;
        final String edgeServicesAnyDeviceReadOnlyClient = "EdgeServices" + DeviceName.ANY_DEVICE + readOnlyClient;
        
        Map<String, String> groupIdToClientMap = new HashMap<String, String>();

        groupIdToClientMap.put("GroupID1", edgeServicesPopRouterClient);
        groupIdToClientMap.put("GroupID2", edgeServicesPopRouterReadOnlyClient);
        groupIdToClientMap.put("GroupID3", lookoutMitigationServiceExplorer);
        groupIdToClientMap.put("GroupID4", edgeServicesAnyDeviceReadOnlyClient);
        
        Map<String, List<String>> operationToClientsMap = new HashMap<String, List<String>>();
        operationToClientsMap.put("CreateMitigation", Lists.newArrayList(edgeServicesPopRouterClient));
        operationToClientsMap.put("ReadMitigations", Lists.newArrayList(edgeServicesPopRouterReadOnlyClient, edgeServicesPopRouterClient));
        operationToClientsMap.put("ListMitigations", Lists.newArrayList(edgeServicesPopRouterReadOnlyClient, edgeServicesPopRouterClient, edgeServicesAnyDeviceReadOnlyClient));
        
        List<String> operationsToAuthWithWriteCredentials = Lists.newArrayList("CreateMitigation");
        authorizer = new AWSUserGroupBasedAuthorizer(groupIdToClientMap, operationToClientsMap, operationsToAuthWithWriteCredentials);
    }
    
    @Test
    public void isValidClientTest() {
        String awsUserGroups = "GroupID1,GroupID2";
        Identity identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        boolean result = authorizer.isValidClient(identity);
        assertTrue(result);
        
        // UserGroups string with a space in between.
        awsUserGroups = "GroupID1, GroupID2";
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        result = authorizer.isValidClient(identity);
        assertTrue(result);
        
        // Unknown userGroups.
        awsUserGroups = "GroupIDSomething";
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        result = authorizer.isValidClient(identity);
        assertFalse(result);
    }
    
    @Test
    public void isClientAuthorizedForServiceAndDeviceTest() {
        String awsUserGroups = "GroupID1";
        Identity identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        // groupid1 has both read write permissions for edgeservices + poprouter
        boolean result = authorizer.isClientAuthorized(identity, "EdgeServices", "PopRouter", "CreateMitigation");
        assertTrue(result);
        
        result = authorizer.isClientAuthorized(identity, "EdgeServices", "PopRouter", "ReadMitigations");
        assertTrue(result);

        // unknown service/device
        result = authorizer.isClientAuthorized(identity, "SomeService", "PopRouter", "ReadMitigations");
        assertFalse(result);

        // groupid2 has read only permissions for edgeservices+poprouter
        awsUserGroups = "GroupID2";
        identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        result = authorizer.isClientAuthorized(identity, "EdgeServices", "PopRouter",  "CreateMitigation");
        assertFalse(result);

        result = authorizer.isClientAuthorized(identity, "EdgeServices", "PopRouter", "ReadMitigations");
        assertTrue(result);

        // Check authorizations for Lookout Mitigation Service Explorer, it
        // should have all permissions for all serviceName+deviceName
        awsUserGroups = "GroupID3";
        identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);

        result = authorizer.isClientAuthorized(identity, "EdgeServices", "PopRouter", "CreateMitigation");
        assertTrue(result);
    }
    
    @Test
    public void isClientAuthorizedForServiceTest() {
        // groupid1 does not have blanket permissions to list mitigations for edgeservices
        String awsUserGroups = "GroupID1";
        Identity identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        boolean result = authorizer.isClientAuthorized(identity, "EdgeServices", "ListMitigations");
        assertFalse(result);
        
        // groupid4 does not have blanket permissions to read mitigations for edgeservices
        awsUserGroups = "GroupID4";
        identity = mock(Identity.class);
        when(identity.getAttribute(Identity.AWS_USER_GROUPS)).thenReturn(awsUserGroups);
        
        result = authorizer.isClientAuthorized(identity, "EdgeServices", "ReadMitigations");
        assertFalse(result);
        
        // groupid4 does have blanket permissions to list mitigations for edgeservices
        
        result = authorizer.isClientAuthorized(identity, "EdgeServices", "ListMitigations");
        assertTrue(result);
    }
    
    @Test
    public void setAuthorizedFlagTest() {
        Identity identity = new Identity();
        authorizer.setAuthorizedFlag(identity, true);
        
        String flag = identity.getAttribute(AWSUserGroupBasedAuthorizer.AUTHORIZED_FLAG_KEY);
        assertTrue(Boolean.valueOf(flag));
        
        authorizer.setAuthorizedFlag(identity, false);
        flag = identity.getAttribute(AWSUserGroupBasedAuthorizer.AUTHORIZED_FLAG_KEY);
        assertFalse(Boolean.valueOf(flag));
    }
}
