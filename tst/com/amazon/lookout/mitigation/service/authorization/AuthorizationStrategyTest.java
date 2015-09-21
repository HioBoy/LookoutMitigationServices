package com.amazon.lookout.mitigation.service.authorization;

import static org.junit.Assert.assertEquals;
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

import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.authorization.AuthorizationStrategy.RequestInfo;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.test.common.util.TestUtils;

@ThreadSafe
public class AuthorizationStrategyTest {
    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging();
    }
    
    private AuthorizationStrategy authStrategy;
    private Context context;
    private CreateMitigationRequest createRequest;
    private DeleteMitigationFromAllLocationsRequest deleteRequest;
    /*private EditMitigationRequest editRequest;*/
    private GetRequestStatusRequest getRequestStatusRequest;
    private ListActiveMitigationsForServiceRequest listMitigationsRequest;
    private GetMitigationInfoRequest getMitigationInfoRequest;
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
    private final String popRouterDeviceName = "POP_ROUTER";
    private final int mitigationVersion = 1;
    private final List<String> locations = new LinkedList<String>();
    
    private final String deviceName = "SomeDevice";
    private final String serviceName = "SomeService";
    private final String mitigationTemplate = "SomeMitigationTemplate";
    
    private final String resourceOwner = "owner";
    private Identity identity;
         
    @Before
    public void setUp() {
        authStrategy = new AuthorizationStrategy(mock(Configuration.class), region);
        
        // initialize create|delete|getRequestStatus|list request objects
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
        
        /*
        editRequest = new EditMitigationRequest();
        updateMitigationModificationRequest(editRequest);
        editRequest.setMitigationDefinition(mock(MitigationDefinition.class));
        editRequest.setMitigationVersion(mitigationVersion);
        editRequest.setLocation(locations);
        */
        
        getRequestStatusRequest = new GetRequestStatusRequest();
        getRequestStatusRequest.setJobId((long) 1);
        getRequestStatusRequest.setServiceName(route53ServiceName);
        getRequestStatusRequest.setDeviceName(popRouterDeviceName);
        
        listMitigationsRequest = new ListActiveMitigationsForServiceRequest();
        listMitigationsRequest.setServiceName(route53ServiceName);
        
        getMitigationInfoRequest = new GetMitigationInfoRequest();
        getMitigationInfoRequest.setServiceName(route53ServiceName);
        getMitigationInfoRequest.setDeviceName(popRouterDeviceName);
        getMitigationInfoRequest.setDeviceScope(DeviceScope.GLOBAL.name());
        getMitigationInfoRequest.setMitigationName(mitigationName);
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
        authInfo.setPolicies(new LinkedList<>());
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
     * resource: arn:<partition>:<vendor>:<region>:<namespace>:<mitigationtemplate>/<servicename>-<devicename>
     */
    // validate the authorization info generated from createMitigationRequest
    @Test
    public void testForCreateMitigationRequest() throws Throwable {
        setOperationNameForContext("CreateMitigation");
        
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, createRequest);
        assertTrue(authInfoList.size() == 1);
         
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-CreateMitigation", "arn:aws:lookout:region::Router_RateLimit_Route53Customer/Route53-POP_ROUTER");
        
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);         
        setArbitraryMitigationTemplate(createRequest);
        boolean accessDeniedException = false;
        try {
            // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
            authInfoList = authStrategy.getAuthorizationInfoList(context, createRequest);            
        } catch (AccessDeniedException e) {
            accessDeniedException = true;
        }
        assertTrue(accessDeniedException);
    }
     
    // validate the authorization info generated from deleteMitigationFromAllLocationsRequest
    @Test
    public void testForDeleteMitigationFromAllLocationsRequest() throws Throwable {
        setOperationNameForContext("DeleteMitigationFromAllLocations");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, deleteRequest);
        assertTrue(authInfoList.size() == 1);
                
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-DeleteMitigationFromAllLocations", "arn:aws:lookout:region::Router_RateLimit_Route53Customer/Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
     
    /*
    @Test
    public void testForEditMitigationRequest() throws Throwable {
        setOperationNameForContext("EditMitigation");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, editRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-EditMitigation", "arn:aws:lookout:region::Router_RateLimit_Route53Customer/Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(authInfo, expectedAuthInfo);
    }
    */
        
    // validate the authorization info generated from getRequestStatusRequest
    @Test
    public void testForGetRequestStatusRequest() throws Throwable {
        setOperationNameForContext("GetRequestStatus");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getRequestStatusRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-GetRequestStatus", "arn:aws:lookout:region::ANY_TEMPLATE/Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    // validate the authorization info generated from listActiveMitigationsForService request
    @Test
    public void testForListMitigationsRequest() throws Throwable {
        setOperationNameForContext("ListActiveMitigationsForService");
        // device name not passed in the request
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, listMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-ListActiveMitigationsForService", "arn:aws:lookout:region::ANY_TEMPLATE/Route53-ANY_DEVICE");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
        
        // set device name in the request
        listMitigationsRequest.setDeviceName(popRouterDeviceName);
        authInfoList = authStrategy.getAuthorizationInfoList(context, listMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-ListActiveMitigationsForService", "arn:aws:lookout:region::ANY_TEMPLATE/Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    // validate the authorization info generated from getMitigationInfoRequest
    @Test
    public void testForGetMitigationInfoRequest() throws Throwable {
        setOperationNameForContext("GetMitigationInfo");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getMitigationInfoRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-GetMitigationInfo", "arn:aws:lookout:region::ANY_TEMPLATE/Route53-POP_ROUTER");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    @Test
    public void testUnrecornizedRequest() {
        // Test the case when Object Request is not a recognizable request
        RequestInfo norequestInfo = AuthorizationStrategy.getRequestInfo("ignored", mock(Object.class));
        assertNull(norequestInfo);
    }

    @Test
    public void testGetRelativeId() {
        // deviceName and mitigationTemplate is null
        String relativeId = AuthorizationStrategy.getMitigationRelativeId(serviceName, null, null);
        assertEquals(AuthorizationStrategy.ANY_TEMPLATE + "/" + serviceName + "-" + DeviceName.ANY_DEVICE, relativeId);

        // deviceName is null
        relativeId = AuthorizationStrategy.getMitigationRelativeId(serviceName, null, mitigationTemplate);
        assertEquals(mitigationTemplate + "/" + serviceName + "-" + DeviceName.ANY_DEVICE, relativeId);

        relativeId = AuthorizationStrategy.getMitigationRelativeId(serviceName, deviceName, mitigationTemplate);
        assertEquals(mitigationTemplate + "/" + serviceName + "-" + deviceName, relativeId);
    }

    @Test
    public void testGenerateActionName() {
        // operations are appended with write- and read- depending on the type of operation.
        // An unknown operation is considered a read request.
        String actionName = AuthorizationStrategy.generateActionName("operation", "prefix");
        assertEquals("lookout:prefix-" + "operation", actionName);
    }
    
    @Test
    public void testGetStrategyName() {
        assertEquals("com.amazon.lookout.mitigation.service.authorization.AuthorizationStrategy", authStrategy.getStrategyName());
    }
}
