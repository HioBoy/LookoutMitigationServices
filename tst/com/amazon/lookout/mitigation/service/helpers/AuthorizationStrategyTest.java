package com.amazon.lookout.mitigation.service.helpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import aws.auth.client.config.Configuration;
import com.amazon.aspen.entity.Policy;
import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
//import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
//import com.amazon.lookout.mitigation.service.ListMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;

@ThreadSafe
public class AuthorizationStrategyTest {
    
    @BeforeClass
    public static void setupOnce() {
        TestUtils.configure();
    }
    
    private AuthorizationStrategy authStrategy;
    private Context context;
    private CreateMitigationRequest createRequest;
    private DeleteMitigationFromAllLocationsRequest deleteRequest;
    private EditMitigationRequest editRequest;
    //private GetRequestStatusRequest getRequestStatusRequest;
    //private ListMitigationsRequest listMitigationsRequest;
    private String region = "region";
     
    /**
     * Note: At this moment LookoutMitigationService supports just one mitigation template
     * and these unit tests are written assuming just its existence. Some of this code would
     * have to be refactored (methods renamed, at least) when other templates and services
     * are handled. 
     */ 
    private final String mitigationName = "test-mitigation-name";
    private final String route35RateLimitMitigationTemplate = MitigationTemplate.Router_RateLimit_Route53Customer;
    private final MitigationActionMetadata mitigationActionMetadata = mock(MitigationActionMetadata.class);
    private final String route53ServiceName = "Route53";
    private final int mitigationVersion = 1;
    private final List<String> locations = new LinkedList<String>();
    
    private final String deviceName = "SomeDevice";
    private final String serviceName = "SomeService";
        
    private final String resourceOwner = "owner";
    private Identity identity;
         
    @Before
    public void setUp() {
        authStrategy = new AuthorizationStrategy(mock(Configuration.class), region);
        
        context = mock(Context.class);
        identity = mock(Identity.class);        
        when(context.getIdentity()).thenReturn(identity);
        when(identity.getAttribute(Identity.AWS_ACCOUNT)).thenReturn(resourceOwner);
        
        createRequest = new CreateMitigationRequest();
        updateMitigationModificationRequest(createRequest);
        createRequest.setMitigationDefinition(mock(MitigationDefinition.class));
        
        deleteRequest = new DeleteMitigationFromAllLocationsRequest();
        updateMitigationModificationRequest(deleteRequest);
        deleteRequest.setMitigationVersion(mitigationVersion);
        
        editRequest = new EditMitigationRequest();
        updateMitigationModificationRequest(editRequest);
        editRequest.setMitigationDefinition(mock(MitigationDefinition.class));
        editRequest.setMitigationVersion(mitigationVersion);
        editRequest.setLocation(locations);
        
        /**
        getRequestStatusRequest = new GetRequestStatusRequest();
        getRequestStatusRequest.setJobId("randomjobid");
        getRequestStatusRequest.setServiceName(serviceName);
        getRequestStatusRequest.setDeviceName(deviceName);
        
        listMitigationsRequest = new ListMitigationsRequest();
        listMitigationsRequest.setServiceName(serviceName);
        */
    }
    
    private void updateMitigationModificationRequest(MitigationModificationRequest modificationRequest) {
        modificationRequest.setMitigationName(mitigationName);
        modificationRequest.setMitigationTemplate(route35RateLimitMitigationTemplate);
        modificationRequest.setMitigationActionMetadata(mitigationActionMetadata);
        modificationRequest.setServiceName(route53ServiceName);
    }
     
    private void setArbitraryMitigationTemplate(MitigationModificationRequest modificationRequest) {
        modificationRequest.setMitigationTemplate("RandomMitigationTemplate");
    }
    
    private void setOperationNameForContext(String operationName) {
        when(context.getOperation()).thenReturn((CharSequence) operationName);
    }
    
    private BasicAuthorizationInfo getBasicAuthorizationInfo(String action, String resource) {
        BasicAuthorizationInfo authInfo = new BasicAuthorizationInfo();
        authInfo.setAction(action);
        authInfo.setResource(resource);
        authInfo.setResourceOwner(resourceOwner);
        authInfo.setPolicies(new LinkedList<Policy>());
        return authInfo;
    }
    
    private void assertEqualAuthorizationInfos(AuthorizationInfo info1, AuthorizationInfo info2) {
        assertEquals(info1.getAction(), info2.getAction());
        assertEquals(info1.getResource(), info2.getResource());
        assertEquals(info1.getResourceOwner(), info2.getResourceOwner());
        assertEquals(info1.getPolicies(), info2.getPolicies());
    }
    
    /**
     * action: <vendor>:read-<operationname> or <vendor>:write-<operationname>
     * resource: arn:<partition>:<vendor>:<region>:<namespace>:<servicename>-<devicename>
     */
     
    @Test
    public void testForCreateMitigationRequest() throws Throwable {
        setOperationNameForContext("CreateMitigation");
        
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, createRequest);
        assertTrue(authInfoList.size() == 1);
         
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-CreateMitigation", "arn:aws:lookout:region::Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
         
        setArbitraryMitigationTemplate(createRequest);
        boolean accessDeniedException = false;
        try {
            // Since MitigationTemplate is not associated to any device, it must result in AccessDeniedException
            authInfoList = authStrategy.getAuthorizationInfoList(context, createRequest);            
        } catch (AccessDeniedException e) {
            accessDeniedException = true;
        }
        assertTrue(accessDeniedException);
    }
     
    @Test
    public void testForDeleteMitigationFromAllLocationsRequest() throws Throwable {
        setOperationNameForContext("DeleteMitigationFromAllLocations");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, deleteRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-DeleteMitigationFromAllLocations", "arn:aws:lookout:region::Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
    }
     
    @Test
    public void testForEditMitigationRequest() throws Throwable {
        setOperationNameForContext("EditMitigation");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, editRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-EditMitigation", "arn:aws:lookout:region::Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
    }
    
    /**
    @Test
    public void testForGetRequestStatusRequest() throws Throwable {
        setOperationNameForContext("GetRequestStatusRequest");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getRequestStatusRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-GetRequestStatusRequest", "arn:aws:lookout:region::"+serviceName+"-"+deviceName);
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
    }
    
    @Test
    public void testForListMitigationsRequest() throws Throwable {
        setOperationNameForContext("ListMitigationsRequest");
        // device name not passed in the request
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, listMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-ListMitigations", "arn:aws:lookout:region::"+serviceName+"-"+DeviceName.ANY_DEVICE);
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
        
        // set device name in the request
        listMitigationsRequest.setDeviceName(deviceName);
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, listMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-ListMitigations", "arn:aws:lookout:region::"+serviceName+"-"+deviceName);
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
    }*/

    @Test
    public void testGenerateResourceName() {
        // Test the case when Object Request is not a recognizable request
        String norequestResource = authStrategy.generateResourceName(mock(Object.class));
        assertNull(norequestResource);
    }

    @Test
    public void testGetRelativeId() {
        // deviceName is null
        String relativeId = authStrategy.getRelativeId(serviceName, null);
        assertEquals(relativeId, serviceName + "-" + DeviceName.ANY_DEVICE);

        relativeId = authStrategy.getRelativeId(serviceName, deviceName);
        assertEquals(relativeId, serviceName + "-" + deviceName);
    }

    @Test
    public void testGenerateActionName() {
        // Only for requests requiring write credentials, operationName is
        // appended with "write-"
        String actionName = authStrategy.generateActionName("operation", mock(Object.class));
        assertEquals(actionName, "lookout:read-" + "operation");
        
        actionName = authStrategy.generateActionName("operation", createRequest);
        assertEquals(actionName, "lookout:write-" + "operation");       
    }
    
    @Test
    public void testGetStrategyName() {
        assertEquals(authStrategy.getStrategyName(), "com.amazon.lookout.mitigation.service.helpers.AuthorizationStrategy");
    }
}