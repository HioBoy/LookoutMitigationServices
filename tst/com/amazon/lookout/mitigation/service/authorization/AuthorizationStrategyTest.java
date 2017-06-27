package com.amazon.lookout.mitigation.service.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import aws.auth.client.config.Configuration;

import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.test.common.util.AssertUtils;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.sqs.model.ListQueuesRequest;

@RunWith(JUnitParamsRunner.class)
public class AuthorizationStrategyTest {
    private static final String TEST_REGION = "test-region";
    private static final String TEST_USER = "12345678910";
    private static final String EXPECTED_ARN_PREFIX = "arn:aws:lookout:" + TEST_REGION + ":" + TEST_USER + ":";
    
    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging();
    }
    
    private AuthorizationStrategy authStrategy;
    private Context context;
    private CreateMitigationRequest createRequest;
    private DeleteMitigationFromAllLocationsRequest deleteRequest;
    private EditMitigationRequest editRequest;
    private GetRequestStatusRequest getRequestStatusRequest;
    private ListActiveMitigationsForServiceRequest listMitigationsRequest;
    private GetMitigationInfoRequest getMitigationInfoRequest;
    private GetLocationHostStatusRequest getLocationHostStatusRequest;
    private ListBlackWatchMitigationsRequest listBlackWatchMitigationsRequest;

    /**
     * Note: At this moment LookoutMitigationService supports just one mitigation template
     * and these unit tests are written assuming just its existence. Some of this code would
     * have to be refactored (methods renamed, at least) when other templates and services
     * are handled. 
     */ 
    private static final String mitigationName = "test-mitigation-name";
    private static final String blackwatchMitigationTemplate = MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer;
    private static final String edgeServiceName = "Edge";
    private static final String blackwatchDeviceName = "BLACKWATCH_POP";
    private static final int mitigationVersion = 1;
    private static final String location = "test-location";
    private static final List<String> locations = new LinkedList<String>();
    
    private final String deviceName = "SomeDevice";
    private final String serviceName = "SomeService";
    private final String mitigationTemplate = "SomeMitigationTemplate";    
    private Identity identity;
         
    @Before
    public void setUp() {
        authStrategy = new AuthorizationStrategy(mock(Configuration.class), TEST_REGION, TEST_USER);
        
        // initialize create|delete|getRequestStatus|list request objects
        context = mock(Context.class);
        identity = mock(Identity.class);
        when(context.getIdentity()).thenReturn(identity);
        when(identity.getAttribute(Identity.AWS_ACCOUNT)).thenReturn(TEST_USER);
        
        createRequest = generateCreateRequest();
        deleteRequest = generateDeleteRequest();
        editRequest = generateEditRequest();
        
        getRequestStatusRequest = new GetRequestStatusRequest();
        getRequestStatusRequest.setJobId((long) 1);
        getRequestStatusRequest.setServiceName(edgeServiceName);
        getRequestStatusRequest.setDeviceName(blackwatchDeviceName);
        
        listMitigationsRequest = new ListActiveMitigationsForServiceRequest();
        listMitigationsRequest.setServiceName(edgeServiceName);
        
        getMitigationInfoRequest = new GetMitigationInfoRequest();
        getMitigationInfoRequest.setServiceName(edgeServiceName);
        getMitigationInfoRequest.setDeviceName(blackwatchDeviceName);
        getMitigationInfoRequest.setMitigationName(mitigationName);
        
        getLocationHostStatusRequest = new GetLocationHostStatusRequest();
        getLocationHostStatusRequest.setLocation(location);
        listBlackWatchMitigationsRequest = new ListBlackWatchMitigationsRequest();
    }

    private static CreateMitigationRequest generateCreateRequest() {
        CreateMitigationRequest createRequest = new CreateMitigationRequest();
        updateMitigationModificationRequest(createRequest);
        createRequest.setMitigationDefinition(mock(MitigationDefinition.class));
        return createRequest;
    }
    
    private static DeleteMitigationFromAllLocationsRequest generateDeleteRequest() {
        DeleteMitigationFromAllLocationsRequest deleteRequest = new DeleteMitigationFromAllLocationsRequest();
        updateMitigationModificationRequest(deleteRequest);
        deleteRequest.setMitigationVersion(mitigationVersion);
        return deleteRequest;
    }
    
    private static EditMitigationRequest generateEditRequest() {
        EditMitigationRequest editRequest = new EditMitigationRequest();
        updateMitigationModificationRequest(editRequest);
        editRequest.setMitigationDefinition(mock(MitigationDefinition.class));
        editRequest.setMitigationVersion(mitigationVersion);
        editRequest.setLocation(locations);
        return editRequest;
    }
    
    private static void updateMitigationModificationRequest(MitigationModificationRequest modificationRequest) {
        modificationRequest.setMitigationName(mitigationName);
        modificationRequest.setMitigationTemplate(blackwatchMitigationTemplate);
        modificationRequest.setMitigationActionMetadata(new MitigationActionMetadata());
        modificationRequest.setServiceName(edgeServiceName);
    }
     
    private void setOperationNameForContext(String operationName) {
        when(context.getOperation()).thenReturn((CharSequence) operationName);
    }
    
    private BasicAuthorizationInfo getBasicAuthorizationInfo(String action, String resource) {
        BasicAuthorizationInfo authInfo = new BasicAuthorizationInfo();
        authInfo.setAction(action);
        authInfo.setResource(resource);
        authInfo.setResourceOwner(TEST_USER);
        authInfo.setPolicies(new LinkedList<>());
        return authInfo;
    }
    
    private void assertEqualAuthorizationInfos(AuthorizationInfo info1, AuthorizationInfo info2) {
        assertEquals(info1.getAction(), info2.getAction());
        assertEquals(info1.getResource(), info2.getResource());
        assertEquals(info1.getResourceOwner(), info2.getResourceOwner());
        assertEquals(info1.getPolicies(), info2.getPolicies());
    }
    
    @SuppressWarnings("unused")
    private static Object[][] getMitigationRequestOperations() {
        return new Object[][] {
                new Object[] { "CreateMitigation", generateCreateRequest() },
                new Object[] { "EditMitigation", generateEditRequest() },
                new Object[] { "DeleteMitigationFromAllLocations", generateDeleteRequest() },
        };
    }
    
    /**
     * action: <vendor>:read-<operationname> or <vendor>:write-<operationname>
     * resource: arn:<partition>:<vendor>:<region>:<namespace>:<mitigationtemplate>/<servicename>-<devicename>
     */
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testValidModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, request);
        assertTrue(authInfoList.size() == 1);
         
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + operation, 
                EXPECTED_ARN_PREFIX + "BlackWatchPOP_PerTarget_EdgeCustomer/Edge-BLACKWATCH_POP");
        
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testBadTemplateForModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        request.setMitigationTemplate("RandomMitigationTemplate");
        
        // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class,
                () -> authStrategy.getAuthorizationInfoList(context, request));
        
        assertThat(exception.getMessage(), containsString("Unrecognized template " + request.getMitigationTemplate()));
    }
    
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testEmptyTemplateForModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        request.setMitigationTemplate("");
        
        // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class,
                () -> authStrategy.getAuthorizationInfoList(context, request));
        
        assertThat(exception.getMessage(), containsString("Missing mitigationTemplate"));
    }
    
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testNullTemplateForModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        request.setMitigationTemplate("");
        
        // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class,
                () -> authStrategy.getAuthorizationInfoList(context, request));
        
        assertThat(exception.getMessage(), containsString("Missing mitigationTemplate"));
    }
    
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testEmptyServiceForModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        request.setServiceName("");
        
        // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class,
                () -> authStrategy.getAuthorizationInfoList(context, request));
        
        assertThat(exception.getMessage(), containsString("Missing serviceName"));
    }
    
    @Test @Parameters(method="getMitigationRequestOperations")
    public void testNullServiceForModificationRequest(String operation, MitigationModificationRequest request) {
        setOperationNameForContext(operation);
        
        request.setServiceName(null);
        
        // Since the arbitrary MitigationTemplate is not associated with any device, it must result in AccessDeniedException
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class,
                () -> authStrategy.getAuthorizationInfoList(context, request));
        
        assertThat(exception.getMessage(), containsString("Missing serviceName"));
    }
    
    // validate the authorization info generated from getRequestStatusRequest
    @Test
    public void testForGetRequestStatusRequest() throws Throwable {
        setOperationNameForContext("GetRequestStatus");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getRequestStatusRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-GetRequestStatus", 
                EXPECTED_ARN_PREFIX + "ANY_TEMPLATE/Edge-BLACKWATCH_POP");
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
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-ListActiveMitigationsForService", 
                EXPECTED_ARN_PREFIX + "ANY_TEMPLATE/Edge-ANY_DEVICE");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
        
        // set device name in the request
        listMitigationsRequest.setDeviceName(blackwatchDeviceName);
        authInfoList = authStrategy.getAuthorizationInfoList(context, listMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-ListActiveMitigationsForService", 
                EXPECTED_ARN_PREFIX + "ANY_TEMPLATE/Edge-BLACKWATCH_POP");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    // validate the authorization info generated from getMitigationInfoRequest
    @Test
    public void testForGetMitigationInfoRequest() throws Throwable {
        setOperationNameForContext("GetMitigationInfo");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getMitigationInfoRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-GetMitigationInfo", 
                EXPECTED_ARN_PREFIX + "ANY_TEMPLATE/Edge-BLACKWATCH_POP");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    // validate the authorization info generated from GetLocationHostStatusRequest
    @Test
    public void testForGetLocationHostStatusRequest() throws Throwable {
        setOperationNameForContext("GetLocationHostStatus");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, getLocationHostStatusRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-GetLocationHostStatus", 
                EXPECTED_ARN_PREFIX + "LOCATION/" + location);
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    // validate the authorization info generated from ListBlackWatchMitigationsRequest
    @Test
    public void testForListBlackWatchMitigationsRequest() throws Throwable {
        setOperationNameForContext("ListBlackWatchMitigations");
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, listBlackWatchMitigationsRequest);
        assertTrue(authInfoList.size() == 1);
        
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:read-ListBlackWatchMitigations", 
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION");
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
    
    @Test
    public void testUnrecornizedRequest() {
        // Request from completely the wrong service
        ListQueuesRequest request = new ListQueuesRequest();
        // Test the case when Object Request is not a recognizable request
        AccessDeniedException exception = AssertUtils.assertThrows(AccessDeniedException.class, 
                () -> AuthorizationStrategy.getRequestInfo("ignored", request));
        
        assertThat(exception.getMessage(), containsString(request.getClass().getName()));
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
        
        // test Location Relative ID
        String locationRelativeId = AuthorizationStrategy.getLocationRelativeId(location);
        assertEquals("LOCATION" + "/" + location, locationRelativeId);
        
        // test BW API Relateive ID
        String bwMitigationRelativeId = AuthorizationStrategy.getBlackWatchAPIRelativeId();
        assertEquals("BLACKWATCH_API/BLACKWATCH_MITIGATION", bwMitigationRelativeId);
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
    
    @Test
    public void testValidAbortDeploymentRequest() throws Throwable {
        setOperationNameForContext("AbortDeployment");
        AbortDeploymentRequest abortRequest = new AbortDeploymentRequest();
        abortRequest.setServiceName(edgeServiceName);
        abortRequest.setDeviceName(blackwatchDeviceName);
        abortRequest.setMitigationTemplate(blackwatchMitigationTemplate);
        abortRequest.setJobId((long) 1);
        
        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, abortRequest);
        assertTrue(authInfoList.size() == 1);
         
        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "AbortDeployment", 
                EXPECTED_ARN_PREFIX + blackwatchMitigationTemplate + "/" + edgeServiceName + "-" + blackwatchDeviceName);
        
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }
}
