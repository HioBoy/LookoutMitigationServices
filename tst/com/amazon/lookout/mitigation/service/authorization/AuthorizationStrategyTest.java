package com.amazon.lookout.mitigation.service.authorization;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import aws.auth.client.config.Configuration;

import com.amazon.balsa.Balsa;
import com.amazon.balsa.engine.Policy;
import com.amazon.balsa.engine.Principal;
import com.amazon.balsa.error.PolicyParseException;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.authorization.AuthorizationStrategy.DestinationIPInfo;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.test.common.util.AssertUtils;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazon.balsa.ContextConstants;
import aws.auth.client.impl.ContextHeuristics;
import aws.auth.client.error.ARCInvalidActionException;

@RunWith(JUnitParamsRunner.class)
public class AuthorizationStrategyTest {
    private static final String TEST_REGION = "us-west-1";
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
    private static final String BLACKWATCH_API_CUSTOM_CONTEXT_TAG = "aws:BlackWatchAPI/TargetIPSpace";

    private final String deviceName = "SomeDevice";
    private final String serviceName = "SomeService";
    private final String mitigationTemplate = "SomeMitigationTemplate";
    private Identity identity;

    private final String CIDR_WITH_128 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/128";
    private final String CIDR_WITH_55 = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/55";
    private final String CIDR_WITH_32 = "52.48.2.1/32";
    private final String CIDR_WITH_24 = "52.53.128.1/24";

    private final String IP_WITH_128 = "2001:db8:85a3:0:0:8a2e:370:7334";
    private final String IP_WITH_55_LOW = "2001:db8:85a3:0:0:0:0:0";
    private final String IP_WITH_55_HIGH = "2001:db8:85a3:1ff:ffff:ffff:ffff:ffff";
    private final String IP_WITH_32 = "52.48.2.1";
    private final String IP_WITH_24_LOW = "52.53.128.0";
    private final String IP_WITH_24_HIGH = "52.53.128.255";

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_1 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"52.53.128.1/24\", "
            + "                  \"52.48.2.1/32\", "
            + "                   \"2001:0db8:85a3:0000:0000:8a2e:0370:7334/55\" "
            + "         ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_2 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"52.53.128.1/24\", "
            + "                  \"52.48.2.1/32\" "
            + "         ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_32 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"52.48.2.1/32\" "
            + "        ], "
            + "        \"mitigation_config\": { }"
            + "    }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_24 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"52.53.128.1/24\" "
            + "        ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_55 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"2001:0db8:85a3:0000:0000:8a2e:0370:7334/55\" "
            + "        ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_24_128 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                 \"52.53.128.1/24\", "
            + "                  \"2001:0db8:85a3:0000:0000:8a2e:0370:7334/128\" "
            + "         ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_IPADDRESSLIST_128 = String.format(""
            + "    {"
            + "        \"destinations\": [ "
            + "                  \"2001:0db8:85a3:0000:0000:8a2e:0370:7334/128\" "
            + "         ], "
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_ELASTICIP = String.format(""
            + "    {"
            + "        \"mitigation_config\": { }"
            + "     }");

    private final String MITIGATION_JSON_CONFIG_REGIONAL = "{"
            + "  \"destinations\": [ "
            + "     \"52.48.2.1/32\" "
            + "    ], "
            + "  \"global_deployment\": {"
            + "    \"placement_tags\": $PLACEMENT_TAGS"
            + "    },"
            + "  \"mitigation_config\": {"
            + "    \"global_traffic_shaper\": {"
            + "      \"default\": {"
            + "        \"global_pps\": 1000,"
            + "        \"global_bps\": 9999"
            + "      }"
            + "    }"
            + "  }"
            + "}";

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

    private BasicAuthorizationInfo getBasicAuthorizationInfo(final String action, final String resource) {
        BasicAuthorizationInfo authInfo = new BasicAuthorizationInfo();
        try {
            authInfo.setActionContext(ContextHeuristics.actionStringToContext(action));
        } catch (final ARCInvalidActionException e) {
            throw new RuntimeException(e);
        }
        authInfo.setResourceContext(ContextHeuristics.resourceArnToContext(resource));

        Map<String, Object> requestContext = new HashMap<>();
        requestContext.put("aws:BlackWatchAPI/ResourceType", null);
        authInfo.setRequestContext(requestContext);

        authInfo.setResourceOwner(TEST_USER);
        authInfo.setPolicies(new LinkedList<>());
        return authInfo;
    }

    // Overloaded method to support destinationIP resource context
    private BasicAuthorizationInfo getBasicAuthorizationInfo(final String action, final String resource, final String ip, 
                                                             final String resourceType) {
        BasicAuthorizationInfo authInfo = getBasicAuthorizationInfo(action, resource);
        Map<String, Object> actionContext = new HashMap<String, Object>();

        if (ip != null) {
            actionContext.put("aws:BlackWatchAPI/TargetIPSpace", ip);
        }
        actionContext.put("aws:BlackWatchAPI/ResourceType", resourceType);
        authInfo.setRequestContext(actionContext);

        return authInfo;
    }

    private BasicAuthorizationInfo getBasicAuthorizationInfo(
            final String action,
            final String resource,
            final String ip,
            final String resourceType,
            final Set<String> placementTags) {
        BasicAuthorizationInfo authInfo = getBasicAuthorizationInfo(action, resource, ip, resourceType);
        if (placementTags != null) {
            authInfo.getRequestContext().put("aws:BlackWatchAPI/PlacementTags", placementTags);
        }
        return authInfo;
    }

    @SneakyThrows
    private static BlackWatchTargetConfig getBlackWatchTargetConfig(ApplyBlackWatchMitigationRequest request) {
        return BlackWatchTargetConfig.fromJSONString(request.getMitigationSettingsJSON());
    }

    @SneakyThrows
    private static BlackWatchTargetConfig getBlackWatchTargetConfig(UpdateBlackWatchMitigationRequest request) {
        return BlackWatchTargetConfig.fromJSONString(request.getMitigationSettingsJSON());
    }

    private void assertEqualAuthorizationInfos(AuthorizationInfo info1, AuthorizationInfo info2) {
        assertEquals(info1.getActionContext().get(ContextConstants.AWS_ACTION),
                info2.getActionContext().get(ContextConstants.AWS_ACTION));
        assertEquals(info1.getResourceContext().get(ContextConstants.AWS_ARN_CTX_KEY),
                info2.getResourceContext().get(ContextConstants.AWS_ARN_CTX_KEY));
        assertEquals(info1.getResourceOwner(), info2.getResourceOwner());
        assertEquals(info1.getPolicies(), info2.getPolicies());
        assertEquals(info1.getRequestContext(), info2.getRequestContext());
    }

    @SuppressWarnings("unused")
    private static Object[][] getMitigationRequestOperations() {
        return new Object[][] {
                new Object[] { "CreateMitigation", generateCreateRequest() },
                new Object[] { "EditMitigation", generateEditRequest() },
                new Object[] { "DeleteMitigationFromAllLocations", generateDeleteRequest() },
        };
    }

    @Test
    public void testArnPartition() {
        String test_region = "us-east-1";
        AuthorizationStrategy strategy = new AuthorizationStrategy(mock(Configuration.class), test_region, TEST_USER);
        String expectedArnPrefix = "arn:aws:lookout:" + test_region + ":" + TEST_USER + ":";
        assertEquals(strategy.getArnPrefix(), expectedArnPrefix);

        test_region = "cn-north-1";
        strategy = new AuthorizationStrategy(mock(Configuration.class), test_region, TEST_USER);
        expectedArnPrefix = "arn:aws-cn:lookout:" + test_region + ":" + TEST_USER + ":";
        assertEquals(strategy.getArnPrefix(), expectedArnPrefix);
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
        String relativeId = AuthorizationStrategy.getMitigationRelativeId(
                serviceName,
                Optional.empty(),
                Optional.empty());
        assertEquals(AuthorizationStrategy.ANY_TEMPLATE + "/" + serviceName + "-" + DeviceName.ANY_DEVICE, relativeId);

        // deviceName is null
        relativeId = AuthorizationStrategy.getMitigationRelativeId(
                serviceName,
                Optional.empty(),
                Optional.of(mitigationTemplate));
        assertEquals(mitigationTemplate + "/" + serviceName + "-" + DeviceName.ANY_DEVICE, relativeId);

        relativeId = AuthorizationStrategy.getMitigationRelativeId(
                serviceName,
                Optional.of(deviceName),
                Optional.of(mitigationTemplate));
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

    /**
     * For a given CIDR, finds the lowest and highest address and adds them to IPList. If CIDR is x.x.x.x/32,
     * x.x.x.x is added to list
     */
    @Test
    public void testPutDestinationIpToList_32() {
        List<String> destinationIpList = new ArrayList<String>();
        AuthorizationStrategy.putDestinationIpToList(CIDR_WITH_32, destinationIpList);
        assertThat(destinationIpList.size(), is(1));
        assertThat(destinationIpList, contains(IP_WITH_32));
    }

    /** For a given CIDR, finds the lowest and highest address and adds them to IPList. If CIDR is x.x.x.x/32,
     * x.x.x.x is added to list
     */
    @Test
    public void testPutDestinationIpToListRange_24() {
        List<String> destinationIpList = new ArrayList<String>();
        AuthorizationStrategy.putDestinationIpToList(CIDR_WITH_24, destinationIpList);
        assertThat(destinationIpList.size(), is(2));
        assertThat(destinationIpList, hasItems(IP_WITH_24_LOW, IP_WITH_24_HIGH));
    }

    /**
     * For a given CIDR, finds the lowest and highest address and adds them to IPList. If CIDR is x.x.x.x/32,
     * x.x.x.x is added to list
     */
    @Test
    public void testPutDestinationIpToList_128() {
        List<String> destinationIpList = new ArrayList<String>();
        AuthorizationStrategy.putDestinationIpToList(CIDR_WITH_128, destinationIpList);
        assertThat(destinationIpList.size(), is(1));
        assertThat(destinationIpList, contains(IP_WITH_128));
    }

    /**
     * For a given CIDR, finds the lowest and highest address and adds them to IPList. If CIDR is x.x.x.x/32,
     * x.x.x.x is added to list
     */
    @Test
    public void testPutDestinationIpToListRange_55() {
        List<String> destinationIpList = new ArrayList<String>();
        AuthorizationStrategy.putDestinationIpToList(CIDR_WITH_55, destinationIpList);
        assertThat(destinationIpList.size(), is(2));
        assertThat(destinationIpList, hasItems(IP_WITH_55_LOW, IP_WITH_55_HIGH));
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddress_32() {
        // IP address Resource Type with x.x.x.x/32 Resource ID, destinationIpList should contain one element

        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_32);

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject = AuthorizationStrategy
                .getApplyBlackWatchMitigationDestinationIPList(applyRequest, getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));

        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(1));
        assertThat(destinationIPInfoObject.getDestinationIPList(), contains(IP_WITH_32));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddress.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddress_24() {
        // IP address Resource Type with x.x.x.x/24 Resource ID, destinationIpList should contain two elements i.e.
        // the lowest and highest address of the /24 range

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_24);
        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(2));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_24_LOW, IP_WITH_24_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddress.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddress_128() {
        // IP address Resource Type with x.x.x.x/128 Resource ID, destinationIpList should contain one element

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_128);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(1));
        assertThat(destinationIPInfoObject.getDestinationIPList(), contains(IP_WITH_128));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddress.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddress_55() {
        // IP address Resource Type with x.x.x.x/55 Resource ID, destinationIpList should contain two elements i.e.
        // the lowest and highest address of the /55 range

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_55);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(2));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_55_LOW, IP_WITH_55_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddress.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddressList_32_24() {
        // IPAddressList Resource type with both /32 and /24 CIDR, destinationIpList should contain three elements i.e.
        // the lowest and highest address of the /24 range and /32 IP Address

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_2);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(3));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_32, IP_WITH_24_LOW, IP_WITH_24_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddressList_32_24_55() {
        // IPAddressList Resource type with /32, /55 and /24 CIDR, destinationIpList should contain 5 elements i.e.
        // the lowest and highest address of the /24 and /55 range and /32 IP Address

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_1);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(5));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_55_LOW, IP_WITH_55_HIGH, IP_WITH_32,
                IP_WITH_24_LOW, IP_WITH_24_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddressList_32() {
        // IPAddressList Resource type with /32 CIDR, destinationIpList should contain one element i.e. /32 IP Address

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_32);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(1));
        assertThat(destinationIPInfoObject.getDestinationIPList(), contains(IP_WITH_32));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_IPAddressList_55() {
        // IPAddressList Resource type with /55 CIDR, destinationIpList should contain 2 elements

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_55);

        destinationIPInfoObject = AuthorizationStrategy.getApplyBlackWatchMitigationDestinationIPList(
                applyRequest,
                getBlackWatchTargetConfig(applyRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(2));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_55_LOW, IP_WITH_55_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * Resource Type is Elastic IP
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_ElasticIP() {
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.ElasticIP.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_55);

        Optional<DestinationIPInfo> destinationIPInfoObject = AuthorizationStrategy
                .getApplyBlackWatchMitigationDestinationIPList(
                        applyRequest,
                        getBlackWatchTargetConfig(applyRequest));
        assertFalse(destinationIPInfoObject.isPresent());
    }

    /** $$ ApplyBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * Resource Type is ELB
     */
    @Test
    public void testGetDestinationIPList_ApplyBWMitigation_ELB() {
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.ELB.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_55);

        Optional<DestinationIPInfo> destinationIPInfoObject = AuthorizationStrategy
                .getApplyBlackWatchMitigationDestinationIPList(
                        applyRequest,
                        getBlackWatchTargetConfig(applyRequest));
        assertFalse(destinationIPInfoObject.isPresent());
    }

    /** $$ UpdateBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_UpdateBWMitigation_IPAddressList_24() {
        // IPAddressList Resource Type with x.x.x.x/24 Resource ID, destinationIpList should contain two elements i.e.
        // the lowest and highest address of the /24 range
        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();
        updateRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_24);

        destinationIPInfoObject = AuthorizationStrategy.getUpdateBlackWatchMitigationDestinationIPList(
                getBlackWatchTargetConfig(updateRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(2));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems(IP_WITH_24_LOW, IP_WITH_24_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ UpdateBlackWatchMitigationRequest $$
     * function to fetch destination list from request
     * For IPAddress resource type: ResourceID is the destination IP
     * For IPAddressList resource type: destinations in mitigation json populate the list
     */
    @Test
    public void testGetDestinationIPList_UpdateBWMitigation_IPAddressList_24_128() {
        // IPAddressList Resource type with both /128 and /24 CIDR, destinationIpList should contain three elements i.e.
        // the lowest and highest address of the /24 range and /128 IP Address

        AuthorizationStrategy.DestinationIPInfo destinationIPInfoObject;
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();
        updateRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_24_128);

        destinationIPInfoObject = AuthorizationStrategy.getUpdateBlackWatchMitigationDestinationIPList(
                getBlackWatchTargetConfig(updateRequest))
                .orElseThrow(() -> new RuntimeException("Empty result"));
        assertThat(destinationIPInfoObject.getDestinationIPList().size(), is(3));
        assertThat(destinationIPInfoObject.getDestinationIPList(), hasItems( IP_WITH_128, IP_WITH_24_LOW, IP_WITH_24_HIGH));
        assertEquals(destinationIPInfoObject.getResourceType(), BlackWatchMitigationResourceType.IPAddressList.name());
    }

    /** $$ UpdateBlackWatchMitigationRequest $$
     * No mitigation json present
     */
    @Test
    public void testGetDestinationIPList_UpdateBWMitigation_No_Config_Json() {
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();

        Optional<DestinationIPInfo> destinationIPInfoObject = AuthorizationStrategy
                .getUpdateBlackWatchMitigationDestinationIPList(getBlackWatchTargetConfig(updateRequest));
        assertFalse(destinationIPInfoObject.isPresent());
    }

    /** $$ UpdateBlackWatchMitigationRequest $$
     * ResourceType is ElasticIP
     * mitigation json present
     */
    @Test
    public void testGetDestinationIPList_UpdateBWMitigation_ElasticIP_2() {
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();
        updateRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_ELASTICIP);

        Optional<DestinationIPInfo> destinationIPInfoObject = AuthorizationStrategy
                .getUpdateBlackWatchMitigationDestinationIPList(getBlackWatchTargetConfig(updateRequest));
        assertFalse(destinationIPInfoObject.isPresent());
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddress
     * Resource ID x.x.x.x/32
     * Resulting authInfoList should contain 1 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddress_32() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_32);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddress.name());

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddress
     * Resource ID x.x.x.x/24
     * Resulting authInfoList should contain 2 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddress_24() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_24);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 2);

        BasicAuthorizationInfo authInfo_1 = (BasicAuthorizationInfo) authInfoList.get(0);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_LOW,
                BlackWatchMitigationResourceType.IPAddress.name()), authInfo_1);

        BasicAuthorizationInfo authInfo_2 = (BasicAuthorizationInfo) authInfoList.get(1);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_HIGH,
                BlackWatchMitigationResourceType.IPAddress.name()), authInfo_2);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddress
     * Resource ID /128
     * Resulting authInfoList should contain 1 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddress_128() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_128);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_128,
                BlackWatchMitigationResourceType.IPAddress.name());

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddress
     * Resource ID ::/55
     * Resulting authInfoList should contain 2 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddress_55() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddress.name());
        applyRequest.setResourceId(CIDR_WITH_55);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 2);

        BasicAuthorizationInfo authInfo_55_low = (BasicAuthorizationInfo) authInfoList.get(0);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_LOW,
                BlackWatchMitigationResourceType.IPAddress.name()), authInfo_55_low);

        BasicAuthorizationInfo authInfo_55_high = (BasicAuthorizationInfo) authInfoList.get(1);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_HIGH,
                BlackWatchMitigationResourceType.IPAddress.name()), authInfo_55_high);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddressList
     * Destination IP x.x.x.x/32
     * Resulting authInfoList should contain 1 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddressList_32() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_32);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name());

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddressList
     * Destination IP x.x.x.x/24
     * Resulting authInfoList should contain 2 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddressList_24() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_24);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 2);

        BasicAuthorizationInfo authInfoStart = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfoStart = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_LOW,
                BlackWatchMitigationResourceType.IPAddressList.name());
        assertEqualAuthorizationInfos(expectedAuthInfoStart, authInfoStart);

        BasicAuthorizationInfo authInfoEnd = (BasicAuthorizationInfo) authInfoList.get(1);
        BasicAuthorizationInfo expectedAuthInfoEnd = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_HIGH,
                BlackWatchMitigationResourceType.IPAddressList.name());
        assertEqualAuthorizationInfos(expectedAuthInfoEnd, authInfoEnd);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddressList
     * Destination IP ::/55
     * Resulting authInfoList should contain 2 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddressList_55() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_55);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 2);

        BasicAuthorizationInfo authInfo_55_low = (BasicAuthorizationInfo) authInfoList.get(0);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_LOW,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_55_low);

        BasicAuthorizationInfo authInfo_55_high = (BasicAuthorizationInfo) authInfoList.get(1);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_HIGH,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_55_high);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddressList
     * Resource ID x.x.x.x/24 , x.x.x.x/32 , ::/55
     * Resulting authInfoList should contain 3 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddressList_1() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_1);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 5);

        BasicAuthorizationInfo authInfo_55_low = (BasicAuthorizationInfo) authInfoList.get(0);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_LOW,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_55_low);

        BasicAuthorizationInfo authInfo_55_high = (BasicAuthorizationInfo) authInfoList.get(1);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_55_HIGH,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_55_high);

        BasicAuthorizationInfo authInfo_32 = (BasicAuthorizationInfo) authInfoList.get(2);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_32);

        BasicAuthorizationInfo authInfo_24_low = (BasicAuthorizationInfo) authInfoList.get(3);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_LOW,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_24_low);

        BasicAuthorizationInfo authInfo_24_high = (BasicAuthorizationInfo) authInfoList.get(4);
        assertEqualAuthorizationInfos(getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_24_HIGH,
                BlackWatchMitigationResourceType.IPAddressList.name()), authInfo_24_high);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type IPAddressList
     * Destination IP ::/128
     * Resulting authInfoList should contain 1 authinfo objects
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_IPAddressList_128() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_128);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", IP_WITH_128,
                BlackWatchMitigationResourceType.IPAddressList.name());

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type ELB
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_ELB() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.ELB.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_1);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", null,null);

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    /**
     * This function tests construction of Authorization Contexts
     * Resource Type ElasticIP
     */
    @Test
    public void testApplyBlackWatchMitigationRequest_ElasticIP_2() {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.ElasticIP.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_IPADDRESSLIST_1);

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo authInfo = (BasicAuthorizationInfo) authInfoList.get(0);
        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo("lookout:write-" + "ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION", null, null);

        assertEqualAuthorizationInfos(expectedAuthInfo, authInfo);
    }

    @Test
    @Parameters({
            "REGIONAL",
            "GLOBAL REGIONAL"
    })
    public void testApplyBlackWatchMitigation_withRegionalPlacementTag(String placementTags) throws Exception {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_REGIONAL.replace(
                "$PLACEMENT_TAGS",
                spaceDelimitedToJsonList(placementTags)));

        AuthorizationStrategy authStrategyUnderTest = new AuthorizationStrategy(
                mock(Configuration.class),
                TEST_REGION,
                TEST_USER,
                ImmutableList.of(
                    "arn:aws:iam::112233445566:role/first_role",
                    "arn:aws:iam::112233445566:role/second_role"));
        List<AuthorizationInfo> authInfoList = authStrategyUnderTest.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo(
                "lookout:write-ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION",
                IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name(),
                ImmutableSet.copyOf(placementTags.split(" ")));
        expectedAuthInfo.setPolicies(singletonList(expectedRegionalPolicy()));
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfoList.get(0));
    }

    @Test
    @Parameters({
            "REGIONAL",
            "GLOBAL REGIONAL"
    })
    public void testApplyBlackWatchMitigation_withRegionalPlacementTag_withNoAllowlistedRoles(String placementTags) {
        setOperationNameForContext("ApplyBlackWatchMitigations");
        ApplyBlackWatchMitigationRequest applyRequest = new ApplyBlackWatchMitigationRequest();
        applyRequest.setResourceType(BlackWatchMitigationResourceType.IPAddressList.name());
        applyRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_REGIONAL.replace(
                "$PLACEMENT_TAGS",
                spaceDelimitedToJsonList(placementTags)));

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, applyRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo(
                "lookout:write-ApplyBlackWatchMitigations",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION",
                IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name(),
                ImmutableSet.copyOf(placementTags.split(" ")));
        expectedAuthInfo.setPolicies(singletonList(expectedRegionalPolicyWithNoAllowlistedRoles()));
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfoList.get(0));
    }

    @Test
    @Parameters({
            "REGIONAL",
            "GLOBAL REGIONAL"
    })
    public void testUpdateBlackWatchMitigation_withRegionalPlacementTag(String placementTags) {
        setOperationNameForContext("UpdateBlackWatchMitigation");
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();
        updateRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_REGIONAL.replace(
                "$PLACEMENT_TAGS",
                spaceDelimitedToJsonList(placementTags)));

        AuthorizationStrategy authStrategyUnderTest = new AuthorizationStrategy(
                mock(Configuration.class),
                TEST_REGION,
                TEST_USER,
                ImmutableList.of(
                        "arn:aws:iam::112233445566:role/first_role",
                        "arn:aws:iam::112233445566:role/second_role"));
        List<AuthorizationInfo> authInfoList = authStrategyUnderTest.getAuthorizationInfoList(context, updateRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo(
                "lookout:write-UpdateBlackWatchMitigation",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION",
                IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name(),
                ImmutableSet.copyOf(placementTags.split(" ")));
        expectedAuthInfo.setPolicies(singletonList(expectedRegionalPolicy()));
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfoList.get(0));
    }

    @Test
    @Parameters({
            "REGIONAL",
            "GLOBAL REGIONAL"
    })
    public void testUpdateBlackWatchMitigation_withRegionalPlacementTag_withNoAllowlistedRoles(String placementTags) {
        setOperationNameForContext("UpdateBlackWatchMitigation");
        UpdateBlackWatchMitigationRequest updateRequest = new UpdateBlackWatchMitigationRequest();
        updateRequest.setMitigationSettingsJSON(MITIGATION_JSON_CONFIG_REGIONAL.replace(
                "$PLACEMENT_TAGS",
                spaceDelimitedToJsonList(placementTags)));

        List<AuthorizationInfo> authInfoList = authStrategy.getAuthorizationInfoList(context, updateRequest);
        assertTrue(authInfoList.size() == 1);

        BasicAuthorizationInfo expectedAuthInfo = getBasicAuthorizationInfo(
                "lookout:write-UpdateBlackWatchMitigation",
                EXPECTED_ARN_PREFIX + "BLACKWATCH_API/BLACKWATCH_MITIGATION",
                IP_WITH_32,
                BlackWatchMitigationResourceType.IPAddressList.name(),
                ImmutableSet.copyOf(placementTags.split(" ")));
        expectedAuthInfo.setPolicies(singletonList(expectedRegionalPolicyWithNoAllowlistedRoles()));
        assertEqualAuthorizationInfos(expectedAuthInfo, authInfoList.get(0));
    }

    private static Policy expectedRegionalPolicy() {
        String policyJson = "{" +
                "  \"Version\" : \"2012-10-17\"," +
                "  \"Statement\" : [ {" +
                "    \"Effect\" : \"Deny\"," +
                "    \"Principal\": \"*\"," +
                "    \"Action\" : [ \"lookout:write-ApplyBlackWatchMitigation\", " +
                "                   \"lookout:write-UpdateBlackWatchMitigation\" ]," +
                "    \"Resource\" : \"arn:aws:lookout:*:*:BLACKWATCH_API/*\"," +
                "    \"Condition\" : {" +
                "      \"ForAnyValue:StringEquals\" : {" +
                "        \"aws:BlackWatchAPI/PlacementTags\" : \"REGIONAL\"" +
                "      }," +
                "      \"ArnNotEquals\" : {" +
                "        \"aws:PrincipalArn\" : [" +
                "           \"arn:aws:iam::112233445566:role/first_role\"," +
                "           \"arn:aws:iam::112233445566:role/second_role\" ]" +
                "      }" +
                "    }" +
                "  } ]" +
                "}";
        return expectedRegionalPolicy(policyJson);
    }


    private static Policy expectedRegionalPolicyWithNoAllowlistedRoles() {
        String policyJson = "{" +
                "  \"Version\" : \"2012-10-17\"," +
                "  \"Statement\" : [ {" +
                "    \"Effect\" : \"Deny\"," +
                "    \"Principal\": \"*\"," +
                "    \"Action\" : [ \"lookout:write-ApplyBlackWatchMitigation\", " +
                "                   \"lookout:write-UpdateBlackWatchMitigation\" ]," +
                "    \"Resource\" : \"arn:aws:lookout:*:*:BLACKWATCH_API/*\"," +
                "    \"Condition\" : {" +
                "      \"ForAnyValue:StringEquals\" : {" +
                "        \"aws:BlackWatchAPI/PlacementTags\" : \"REGIONAL\"" +
                "      }" +
                "    }" +
                "  } ]" +
                "}";
        return expectedRegionalPolicy(policyJson);
    }

    private static Policy expectedRegionalPolicy(String policyJson) {
        try {
            Policy policy = Balsa.getInstance().parse(new StringReader(policyJson));
            policy.setIssuer(new Principal(TEST_USER, emptySet()));
            return policy;
        } catch (PolicyParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static String spaceDelimitedToJsonList(String value) {
        return Arrays.stream(value.split(" "))
                .map(tag -> String.format("\"%s\"", tag))
                .collect(Collectors.joining(",", "[", "]"));
    }
}
