package com.amazon.lookout.mitigation.service.authorization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import com.amazon.balsa.engine.Policy;
import com.amazon.balsa.engine.Principal;
import com.amazon.balsa.proto.Balsa.ActionBlock;
import com.amazon.balsa.proto.Balsa.ConditionBlock;
import com.amazon.balsa.proto.Balsa.ConditionEntry;
import com.amazon.balsa.proto.Balsa.ConditionKey;
import com.amazon.balsa.proto.Balsa.ConditionKey.KeyType;
import com.amazon.balsa.proto.Balsa.ConditionQualifier;
import com.amazon.balsa.proto.Balsa.ConditionType;
import com.amazon.balsa.proto.Balsa.ConditionValue;
import com.amazon.balsa.proto.Balsa.ConditionValue.ConditionValueType;
import com.amazon.balsa.proto.Balsa.Effect;
import com.amazon.balsa.proto.Balsa.PolicyBlock;
import com.amazon.balsa.proto.Balsa.PrincipalMapBlock;
import com.amazon.balsa.proto.Balsa.PrincipalMapEntry;
import com.amazon.balsa.proto.Balsa.Provider;
import com.amazon.balsa.proto.Balsa.ResourceBlock;
import com.amazon.balsa.proto.Balsa.ResourceValue;
import com.amazon.balsa.proto.Balsa.ResourceValue.ResourceType;
import com.amazon.balsa.proto.Balsa.StatementBlock;
import com.amazon.balsa.proto.Balsa.Version;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationStateRequest;
import com.amazon.lookout.mitigation.service.GetLocationOperationalStatusRequest;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementRequest;
import com.amazon.lookout.mitigation.service.UpdateLocationStateRequest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Getter;

import lombok.Value;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import aws.auth.client.config.Configuration;
import aws.auth.client.impl.ContextHeuristics;
import aws.auth.client.error.ARCInvalidActionException;

import com.amazon.blackwatch.helper.CIDRUtils;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AbstractAwsAuthorizationStrategy;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.DeactivateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.aws.rip.RIPHelper;
import com.aws.rip.models.exception.RegionIsInTestException;
import com.aws.rip.models.exception.RegionNotFoundException;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

/**
 * AuthorizationStrategy looks at the context and request to generate action and resource names to
 * be used by AuthorizationHandler to make authorization decisions. The AuthorizationHandler looks up
 * IAM policies associated with the requesting IAM user (request is signed with sig v4 using the credentials
 * of this user) or one of the IAM group that this user belongs to to make a decision to allow or deny request.
 *
 * An example IAM policy is shown below.
 *
 * {
 * "Version": "2012-10-17",
 * "Statement": [
 *   {
 *     "Effect": "Allow",
 *     "Action": "lookout:write-DeleteMitigationFromAllLocations",
 *     "Resource": "arn:aws:lookout:us-east-1:namespace:Router_RateLimit_Route53Customer/Route53-POP_ROUTER"
 *   }
 * ]
 * }
 * Wild cards can be used to match multiple action and resource names. E.g.,
 * "Action": "lookout:write-*"
 *
 * We use the following naming convention for action and resource:
 * action: <vendor>:read-<operationname> or <vendor>:write-<operationname>
 * resource: arn:<partition>:<vendor>:<region>:<namespace>:<mitigationtemplate>/<servicename>-<devicename>
 */
@ThreadSafe
public class AuthorizationStrategy extends AbstractAwsAuthorizationStrategy {
    /**
     * Some APIs do not concern with MitigationTemplate. In such cases we use ANY_TEMPLATE constant in the ARN.
     */
    protected static final String ANY_TEMPLATE = "ANY_TEMPLATE";

    private static final Log LOG = LogFactory.getLog(AuthorizationStrategy.class);

    /**
     * Some operations such as GetRequestStatus are not write operations but
     * are only relevant when they follow write operations such as CreateMitigation,
     * are only authorized to those who have permissions for the latter write operations.
     * The action names for such operations would be generated with write_operation_prefix.
     */
    private static final String WRITE_OPERATION_PREFIX = "write";
    private static final String READ_OPERATION_PREFIX = "read";

    private static final String BLACKWATCH_API_RESOURCE_PREFIX = "BLACKWATCH_API";
    private static final String BLACKWATCH_MITIGATION_RESOURCE_PREFIX = "BLACKWATCH_MITIGATION";
    private static final String BLACKWATCH_API_TARGET_IP_SPACE_TAG = "aws:BlackWatchAPI/TargetIPSpace";
    private static final String BLACKWATCH_API_RESOURCE_TYPE_TAG = "aws:BlackWatchAPI/ResourceType";
    private static final String BLACKWATCH_API_PLACEMENT_TAGS = "aws:BlackWatchAPI/PlacementTags";

    // Constants used for generating ARN
    private static final String ARN_VENDOR = "lookout";
    private static final String ARN_SEPARATOR = "-";

    private static final ConcurrentHashMap<Class<?>, RequestInfoFunction<Object>> requestInfoParsers =
            initRequestInfoParsers();

    private final String ownerAccountId;

    @Getter
    private final String arnPrefix;

    private final Policy regionalMitigationPolicy;

    public AuthorizationStrategy(Configuration arcConfig, String region, String ownerAccountId) {
        this(arcConfig, region, ownerAccountId, ImmutableList.of());
    }

    public AuthorizationStrategy(
            Configuration arcConfig,
            String region,
            String ownerAccountId,
            List<String> regionalMitigationsRoleAllowlist) {
        super(arcConfig);
        Validate.notNull(arcConfig);
        Validate.notEmpty(region);
        Validate.notEmpty(ownerAccountId);
        this.ownerAccountId = ownerAccountId;
        String partition = "";
        try {
            partition = RIPHelper.getRegion(region).getArnPartition();
            LOG.info("Region: " + region + ", arn partition: " + partition);
        } catch (RegionNotFoundException e) {
            LOG.error("Region " + region + " doesn't exist");
        } catch (RegionIsInTestException e) {
            LOG.error("Region " + region + " is still under testing");
        }
        this.arnPrefix = "arn:" + partition + ":" + ARN_VENDOR + ":" + region + ":" + ownerAccountId + ":";

        this.regionalMitigationPolicy = generateRegionalMitigationPolicy(
                regionalMitigationsRoleAllowlist,
                ownerAccountId);
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getName();
    }

    /**
     * (non-Javadoc)
     * @see {@link com.amazon.coral.service.AbstractAwsAuthorizationStrategy#getAuthorizationInfoList(
     * com.amazon.coral.service.Context, java.lang.Object)
     *
     * LookoutMitigationService authorization scheme authorizes clients by ServiceName+DeviceName combinations
     * for each operation/API. Lookout creates IAM users for each ServiceName+DeviceName+[Read|Write] combination,
     * and grants those users permissions to the respective ServiceName+DeviceName+[Read|Write] combination
     * through IAM policies. Optionally, permissions may be MitigationTemplate specific too. [Read|Write]
     * distinguishes read and write credentials respectively. The requests to LookoutMitigationService are
     * supposed to be then signed by credentials of appropriate IAM user depending of course on the requested
     * API and parameters. The credentials of the above IAM users are maintained and distributed through ODIN.
     *
     * The serviceName+deviceName information, and optionally serviceName+deviceName+mitigationTemplate, is
     * encoded in the resourceName. Whereas [Read|Write] information is included in the actionName along with the
     * operationName. Typically, IAM users with write permissions also have read permissions.
     *
     * getAuthorizationInfoList looks into the incoming request and operation and returns resourceName and actionName
     * in an authorizationInfo.
     */
    @Override
    public List<AuthorizationInfo> getAuthorizationInfoList(Context context, Object request)
            throws AccessDeniedException {
        RequestInfo requestInfo = getRequestInfo(context.getOperation().toString(), request);
        if (requestInfo == null) {
            throw new RuntimeException("Failed getting request info for request " + request);
        }

        String resourceName = arnPrefix + requestInfo.getRelativeArn();

        LOG.debug("Action: " + requestInfo.getAction() + " ; " + "Resource (ARN): " + resourceName);

        List<AuthorizationInfo> authInfoList = new LinkedList<>();
        if (requestInfo.getDestinationIPInfoObject().isPresent()) {
            DestinationIPInfo destinationIPInfo = requestInfo.getDestinationIPInfoObject().get();
            for (String ip : destinationIPInfo.getDestinationIPList()) {
                authInfoList.add(getAuthorizationInfo(
                        requestInfo,
                        resourceName,
                        Optional.of(ip),
                        Optional.of(destinationIPInfo.getResourceType())));
            }
        } else {
            // if getDestinationIPList() is empty; implies ResourceType is neither IPAddress or IPAddressList
            authInfoList.add(getAuthorizationInfo(requestInfo, resourceName, Optional.empty(), Optional.empty()));
        }

        return authInfoList;
    }

    /**
     * Constructs one BasicAuthorizationInfo object to be added to AuthorizationInfoList in getAuthorizationInfoList()
     * @param ip - applicable only for IPAddress and IPAddressList Resource Type
     */
    private BasicAuthorizationInfo getAuthorizationInfo(
            RequestInfo requestInfo,
            String resourceName,
            Optional<String> ip,
            Optional<String> resourceType) {
        BasicAuthorizationInfo authorizationInfo = new BasicAuthorizationInfo();

        // Action that need to be authorized
        try {
            authorizationInfo.setActionContext(ContextHeuristics.actionStringToContext(
                    requestInfo.getAction()));
        } catch (final ARCInvalidActionException e) {
            throw new IllegalArgumentException(e);
        }

        // Resource that is guarded
        authorizationInfo.setResourceContext(ContextHeuristics.resourceArnToContext(
                resourceName));

        // Principal identifier of the resource owner
        // associated with this authorization call
        authorizationInfo.setResourceOwner(ownerAccountId);

        Map<String, Object> requestContext = new HashMap<>();

        // destination ip to be evaluated against IPSpace listed in IAM policy
        // https://sim.amazon.com/issues/BLACKWATCH-2900
        ip.ifPresent(ipValue -> requestContext.put(BLACKWATCH_API_TARGET_IP_SPACE_TAG, ipValue));

        // PlacementTags is a multivalued condition key. IAM policies that use it should use ForAllValues and
        // ForAnyValue condition operators.
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_multi-value-conditions.html#reference_policies_multi-key-or-value-conditions
        requestInfo.getPlacementTags().ifPresent(placementTags ->
                requestContext.put(BLACKWATCH_API_PLACEMENT_TAGS, placementTags));

        if (requestInfo.getPlacementTags().isPresent() &&
                requestInfo.getPlacementTags().get().contains(BlackWatchTargetConfig.REGIONAL_PLACEMENT_TAG)) {
            LOG.debug("Applying additional IAM policy to deny REGIONAL mitigation unless the caller IAM role is " +
                    "explicitly allowlisted: " + toJson(regionalMitigationPolicy));
            authorizationInfo.setPolicies(singletonList(regionalMitigationPolicy));
        } else {
            authorizationInfo.setPolicies(emptyList());
        }

        // if applyBWmitigation and updateBWmitigation is called we are interested in the resourcetype as well, in all
        // other cases it will be null
        requestContext.put(BLACKWATCH_API_RESOURCE_TYPE_TAG, resourceType.orElse(null));
        authorizationInfo.setRequestContext(requestContext);

        return authorizationInfo;
    }

    private static Policy generateRegionalMitigationPolicy(List<String> roleAllowlist, String ownerAccountId) {
        // If request PlacementTags contain REGIONAL tag then explicitly deny all principals except for the ones
        // specified in the roleAllowlist policy condition.
        // This is a resource-based policy.
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_identity-vs-resource.html
        // https://w.amazon.com/bin/view/AWSAuth/AccessManagement/Aspen/Resource_Policies
        ConditionBlock.Builder conditionBuilder = ConditionBlock.newBuilder()
                .addEntry(ConditionEntry.newBuilder()
                        .setQualifier(ConditionQualifier.FOR_ANY_VALUE)
                        .setType(ConditionType.STRING_EQUALS)
                        .setKey(ConditionKey.newBuilder()
                                .setKeyType(KeyType.STRING)
                                .setStringKey(BLACKWATCH_API_PLACEMENT_TAGS))
                        .addValue(ConditionValue.newBuilder()
                                .setType(ConditionValueType.STRING)
                                .setString(BlackWatchTargetConfig.REGIONAL_PLACEMENT_TAG)));
        if (!roleAllowlist.isEmpty()) {
            conditionBuilder.addEntry(ConditionEntry.newBuilder()
                    .setQualifier(ConditionQualifier.SINGLE_VALUE)
                    .setType(ConditionType.ARN_NOT_EQUALS)
                    .setKey(ConditionKey.newBuilder()
                            .setKeyType(KeyType.STRING)
                            // If the caller uses an assumed role then aws:PrincipalArn condition key matches ARN of the
                            // IAM role (example: `arn:aws:iam::AWS-account-ID:role/role-name`), not ARNs of the assumed
                            // role (example: `arn:aws:sts::AWS-account-ID:assumed-role/role-name/role-session-name`).
                            // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-principalarn
                            .setStringKey("aws:PrincipalArn"))
                    .addAllValue(roleAllowlist.stream()
                            .map(role -> ConditionValue.newBuilder()
                                    .setType(ConditionValueType.STRING)
                                    .setString(role)
                                    .build())
                            .collect(Collectors.toList())));
        }

        Policy policy = new Policy(PolicyBlock.newBuilder()
                .setVersion(Version.V_2012_10_17)
                .addStatement(StatementBlock.newBuilder()
                        .setEffect(Effect.DENY)
                        // Principal is required in a resource-based policy: "You must use the Principal element in
                        // resource-based policies.", but "When you specify an assumed-role session in a Principal
                        // element, you cannot use a wildcard "*" to mean all sessions. Principals must always name a
                        // specific session." We cannot use a specific session in the "Principal" block because clients
                        // autogenerate session names. So we use "*" in the "Principal" block and use aws:PrincipalArn
                        // condition to match only allowlisted principals.
                        // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
                        .setPrincipalMapBlock(PrincipalMapBlock.newBuilder()
                                .addEntry(PrincipalMapEntry.newBuilder()
                                        .setProvider(Provider.STAR)
                                        .addPrincipal("*")))
                        .setActionBlock(ActionBlock.newBuilder()
                                .addAction("lookout:write-ApplyBlackWatchMitigation")
                                .addAction("lookout:write-UpdateBlackWatchMitigation"))
                        .setResourceBlock(ResourceBlock.newBuilder()
                                .addResource(ResourceValue.newBuilder()
                                        .setType(ResourceType.STRING)
                                        .setString("arn:aws:lookout:*:*:BLACKWATCH_API/*")))
                        .setConditionBlock(conditionBuilder))
                .build());
        policy.setIssuer(new Principal(ownerAccountId, emptySet()));
        return policy;
    }

    private static String toJson(Policy policy) {
        try {
            return policy.toPrettyJsonString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ConcurrentHashMap<Class<?>, RequestInfoFunction<Object>> initRequestInfoParsers() {
        ConcurrentHashMap<Class<?>, RequestInfoFunction<Object>> parsersByRequestType = new ConcurrentHashMap<>();

        // All MitigationModificationRequests share authorization policy
        addRequestInfoParser(
                parsersByRequestType,
                MitigationModificationRequest.class,
                (action, request) -> {
                    String mitigationTemplate = request.getMitigationTemplate();
                    if (StringUtils.isEmpty(mitigationTemplate)) {
                        throw new AccessDeniedException("Missing mitigationTemplate");
                    }

                    DeviceName deviceName =
                            MitigationTemplateToDeviceMapper.getDeviceNameForTemplate(mitigationTemplate);
                    if (deviceName == null) {
                        throw new AccessDeniedException("Unrecognized template " + mitigationTemplate);
                    }

                    String serviceName = request.getServiceName();
                    if (StringUtils.isEmpty(serviceName)) {
                        throw new AccessDeniedException("Missing serviceName");
                    }

                    return generateMitigationRequestInfo(
                            action,
                            WRITE_OPERATION_PREFIX,
                            serviceName,
                            Optional.of(deviceName.name()),
                            Optional.of(mitigationTemplate));
                });

        // abort deployment authorization policy
        addRequestInfoParser(
                parsersByRequestType,
                AbortDeploymentRequest.class,
                (action, request) -> {
                    String mitigationTemplate = request.getMitigationTemplate();
                    if (StringUtils.isEmpty(mitigationTemplate)) {
                        throw new AccessDeniedException("Missing mitigationTemplate");
                    }

                    DeviceName deviceName =
                            MitigationTemplateToDeviceMapper.getDeviceNameForTemplate(mitigationTemplate);
                    if (deviceName == null) {
                        throw new AccessDeniedException("Unrecognized template " + mitigationTemplate);
                    }

                    String serviceName = request.getServiceName();
                    if (StringUtils.isEmpty(serviceName)) {
                        throw new AccessDeniedException("Missing serviceName");
                    }

                    return generateMitigationRequestInfo(
                            action,
                            WRITE_OPERATION_PREFIX,
                            serviceName,
                            Optional.of(deviceName.name()),
                            Optional.of(mitigationTemplate));
                });

        addRequestInfoParser(
                parsersByRequestType,
                GetRequestStatusRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                ListActiveMitigationsForServiceRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                GetMitigationInfoRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                GetMitigationDefinitionRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                GetMitigationHistoryRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                GetLocationDeploymentHistoryRequest.class,
                (action, request) ->
                        generateMitigationRequestInfo(
                                action,
                                READ_OPERATION_PREFIX,
                                request.getServiceName(),
                                Optional.ofNullable(request.getDeviceName())));

        addRequestInfoParser(
                parsersByRequestType,
                GetLocationHostStatusRequest.class,
                (action, request) ->
                        generateHostStatusRequestInfo(action, READ_OPERATION_PREFIX, request.getLocation()));

        addRequestInfoParser(
                parsersByRequestType,
                GetLocationOperationalStatusRequest.class,
                (action, request) ->
                        new RequestInfo(
                                generateActionName(action, READ_OPERATION_PREFIX),
                                getBlackWatchAPIRelativeId()));

        addRequestInfoParser(
                parsersByRequestType,
                RequestHostStatusChangeRequest.class,
                (action, request) ->
                        generateHostStatusRequestInfo(action, WRITE_OPERATION_PREFIX, request.getLocation()));

        addRequestInfoParser(
                parsersByRequestType,
                ListBlackWatchMitigationsRequest.class,
                (action, request) ->
                        generateListBlackWatchMitigationRequestInfo(action, READ_OPERATION_PREFIX));

        addRequestInfoParser(
                parsersByRequestType,
                DeactivateBlackWatchMitigationRequest.class,
                (action, request) ->
                        new RequestInfo(
                                generateActionName(action, WRITE_OPERATION_PREFIX),
                                getBlackWatchAPIRelativeId()));

        addRequestInfoParser(
                parsersByRequestType,
                ChangeBlackWatchMitigationOwnerARNRequest.class,
                (action, request) ->
                        new RequestInfo(
                                generateActionName(action, WRITE_OPERATION_PREFIX),
                                getBlackWatchAPIRelativeId()));

        addRequestInfoParser(
                parsersByRequestType,
                ApplyBlackWatchMitigationRequest.class,
                (action, request) ->
                        generateApplyBlackWatchMitigationRequestInfo(action, WRITE_OPERATION_PREFIX, request));

        addRequestInfoParser(
                parsersByRequestType,
                UpdateBlackWatchMitigationRequest.class,
                (action, request) ->
                        generateUpdateBlackWatchMitigationRequestInfo(action, WRITE_OPERATION_PREFIX, request));

        addRequestInfoParser(
                parsersByRequestType,
                ListBlackWatchLocationsRequest.class,
                (action, request) ->
                        generateLocationStateRequestInfo(action, READ_OPERATION_PREFIX));

        addRequestInfoParser(
                parsersByRequestType,
                UpdateBlackWatchLocationStateRequest.class,
                (action, request) ->
                        generateLocationStateRequestInfo(action, WRITE_OPERATION_PREFIX));

        addRequestInfoParser(
                parsersByRequestType,
                UpdateLocationStateRequest.class,
                (action, request) ->
                        generateLocationStateRequestInfo(action, WRITE_OPERATION_PREFIX));

        addRequestInfoParser(
                parsersByRequestType,
                ChangeBlackWatchMitigationStateRequest.class,
                (action, request) ->
                        new RequestInfo(
                                generateActionName(action, WRITE_OPERATION_PREFIX),
                                getBlackWatchAPIRelativeId()));

        addRequestInfoParser(
                parsersByRequestType,
                UpdateBlackWatchMitigationRegionalCellPlacementRequest.class,
                (action, request) ->
                        new RequestInfo(
                                generateActionName(action, WRITE_OPERATION_PREFIX),
                                getBlackWatchAPIRelativeId()));

        return parsersByRequestType;
    }

    @SuppressWarnings("unchecked")
    private static <T> void addRequestInfoParser(
            ConcurrentHashMap<Class<?>, RequestInfoFunction<Object>> parsersByRequestType,
            Class<T> clazz,
            RequestInfoFunction<T> function) {
        if (parsersByRequestType.containsKey(clazz)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " was already registered");
        }
        parsersByRequestType.put(clazz, (RequestInfoFunction<Object>) function);
    }

    private static RequestInfo generateMitigationRequestInfo(
            String action,
            String prefix,
            String serviceName,
            Optional<String> deviceName) {
        return generateMitigationRequestInfo(action, prefix, serviceName, deviceName, Optional.empty());
    }

    private static RequestInfo generateMitigationRequestInfo(
            String action,
            String prefix,
            String serviceName,
            Optional<String> deviceName,
            Optional<String> mitigationTemplate) {
        return new RequestInfo(
                generateActionName(action, prefix),
                getMitigationRelativeId(serviceName, deviceName, mitigationTemplate));
    }

    private static RequestInfo generateHostStatusRequestInfo(String action, String prefix, String locationName) {
        return new RequestInfo(
                generateActionName(action, prefix),
                getLocationRelativeId(locationName));
    }

    private static RequestInfo generateListBlackWatchMitigationRequestInfo(String action, String prefix) {
        return new RequestInfo(
                generateActionName(action, prefix),
                getBlackWatchAPIRelativeId());
    }

    private static RequestInfo generateApplyBlackWatchMitigationRequestInfo(
            String action,
            String prefix,
            ApplyBlackWatchMitigationRequest request) {
        BlackWatchTargetConfig targetConfig = parseTargetConfig(request.getMitigationSettingsJSON());
        return new RequestInfo(
                generateActionName(action, prefix),
                getBlackWatchAPIRelativeId(),
                getApplyBlackWatchMitigationDestinationIPList(request, targetConfig),
                getPlacementTags(targetConfig));
    }

    private static RequestInfo generateUpdateBlackWatchMitigationRequestInfo(
            String action,
            String prefix,
            UpdateBlackWatchMitigationRequest request) {
        BlackWatchTargetConfig targetConfig = parseTargetConfig(request.getMitigationSettingsJSON());
        return new RequestInfo(
                generateActionName(action, prefix),
                getBlackWatchAPIRelativeId(),
                getUpdateBlackWatchMitigationDestinationIPList(targetConfig),
                getPlacementTags(targetConfig));
    }

    private static BlackWatchTargetConfig parseTargetConfig(String mitigationSettingsJson) {
        try {
            return BlackWatchTargetConfig.fromJSONString(mitigationSettingsJson);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Could not map mitigation JSON to target config: " +
                    ex.getMessage(), ex);
        }
    }

    private static Optional<ImmutableSet<String>> getPlacementTags(BlackWatchTargetConfig targetConfig) {
        return Optional.ofNullable(targetConfig.getGlobal_deployment())
                .flatMap(it -> Optional.ofNullable(it.getPlacement_tags()))
                .map(ImmutableSet::copyOf);
    }

    private static RequestInfo generateLocationStateRequestInfo(String action, String prefix) {
        return new RequestInfo(
                generateActionName(action, prefix),
                getBlackWatchAPIRelativeId());
    }

    private static RequestInfoFunction<Object> getRequestInfoFunction(Object request) {
        RequestInfoFunction<Object> function = requestInfoParsers.get(request.getClass());
        if (function != null) {
            return function;
        }

        function = requestInfoParsers.entrySet().stream()
                .filter(e -> e.getKey().isInstance(request))
                .findFirst()
                .map(e -> e.getValue())
                .orElseThrow(() -> new AccessDeniedException("Unrecognized action: " + request.getClass().getName()));

        requestInfoParsers.put(request.getClass(), function);

        return function;
    }

    static RequestInfo getRequestInfo(String action, Object request) {
        return getRequestInfoFunction(request).getRequestInfo(action, request);
    }

    /**
     * serviceName+deviceName+mitigationTemplate combination is included in the relative identifier as:
     * mitigationTemplate/serviceName-deviceName if mitigationTemplate is not null, else as: serviceName-deviceName.
     *
     * mitigationTemplate may be non-null to selectively authorize MitigationModificationRequests for some
     * serviceName+deviceName combinations. E.g., different sets of users may be authorized for applying
     * ratelimit and count filters on routers.
     */
    static String getMitigationRelativeId(
            String serviceName,
            Optional<String> deviceName,
            Optional<String> mitigationTemplate) {
        return new StringBuilder()
                .append(mitigationTemplate.orElse(ANY_TEMPLATE))
                .append("/")
                .append(serviceName)
                .append(ARN_SEPARATOR)
                /*
                  for some request types deviceName is not a required field. In those cases deviceName is
                  set to ANY_DEVICE, granting authorization for all devices, or none.
                 */
                .append(deviceName.orElse(DeviceName.ANY_DEVICE.name()))
                .toString();
    }

    /**
     * locationName is included in the relative identifier as:
     * LOCATION/locationName
     * locationName should be non-null
     */
    static String getLocationRelativeId(String locationName) {
        return new StringBuilder()
                .append("LOCATION")
                .append("/")
                .append(locationName)
                .toString();
    }

    /**
     * the relative identifier:
     * BLACKWATCH_MITIGATION/MITIGATION_STATE
     */
    static String getBlackWatchAPIRelativeId() {
        return BLACKWATCH_API_RESOURCE_PREFIX + "/" + BLACKWATCH_MITIGATION_RESOURCE_PREFIX;
    }

    /**
     * Generate action name with the following structure:
     * action: <vendor>:prefix-<operationname>
     */
    static String generateActionName(String operationName, String prefix) {
        Validate.notNull(operationName);
        Validate.notNull(prefix);

        return new StringBuilder()
                .append(ARN_VENDOR)
                .append(":")
                .append(prefix)
                .append(ARN_SEPARATOR)
                .append(operationName)
                .toString();
    }

    /**
     * Function to extract destination IPs from the request context for ApplyBlackWatchMitigation
     * @return List of destination ips fetched from the request
     */
    static Optional<DestinationIPInfo> getApplyBlackWatchMitigationDestinationIPList(
            ApplyBlackWatchMitigationRequest request,
            BlackWatchTargetConfig targetConfig) {
        List<String> destinationIpList = new ArrayList<>();

        // If ResourceType is IPAddress
        if (BlackWatchMitigationResourceType.IPAddress.name().equals(request.getResourceType())) {
            putDestinationIpToList(request.getResourceId(), destinationIpList);

            return Optional.of(new DestinationIPInfo(
                    BlackWatchMitigationResourceType.IPAddress.name(),
                    ImmutableList.copyOf(destinationIpList)));
        } else if (BlackWatchMitigationResourceType.IPAddressList.name().equals(request.getResourceType())) {
            // If ResourceType is IPAddressList we need to extract destinations from mitigation json
            if (!CollectionUtils.isEmpty(targetConfig.getDestinations())) {
                for (String ip : targetConfig.getDestinations()) {
                    putDestinationIpToList(ip, destinationIpList);
                }
            }

            return Optional.of(new DestinationIPInfo(
                    BlackWatchMitigationResourceType.IPAddressList.name(),
                    ImmutableList.copyOf(destinationIpList)));
        }

        return Optional.empty();
    }

    /**
     * Function to extract destination IPs from the request context for UpdateBlackWatchMitigation
     * @return List of destination ips fetched from the request
     */
    static Optional<DestinationIPInfo> getUpdateBlackWatchMitigationDestinationIPList(
            BlackWatchTargetConfig targetConfig) {
        List<String> destinationIpList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(targetConfig.getDestinations())) {
            for (String ip : targetConfig.getDestinations()) {
                putDestinationIpToList(ip, destinationIpList);
            }

            return Optional.of(new DestinationIPInfo(
                    BlackWatchMitigationResourceType.IPAddressList.name(),
                    ImmutableList.copyOf(destinationIpList)));
        }

        return Optional.empty();
    }

    /**
     * For a given CIDR, finds the lowest and highest address and adds them to destinationIpList. If CIDR is x.x.x.x/32,
     * x.x.x.x is added to list(same for ipv6 /128)
     */
    static void putDestinationIpToList(String ip, List<String> destinationIpList) {
        CIDRUtils cidr;
        try {
            cidr = new CIDRUtils(ip);
        } catch (Exception ex) {
            throw new RuntimeException("Exception when determining destination ip: " + ip, ex);
        }

        Set<String> tmpIPList = new HashSet<>();
        tmpIPList.add(cidr.getNetworkAddress());
        tmpIPList.add(cidr.getBroadcastAddress());

        // Add all to destination IP List
        destinationIpList.addAll(tmpIPList);
    }

    @Value
    @AllArgsConstructor
    static class DestinationIPInfo {
        private final String resourceType;
        private final ImmutableList<String> destinationIPList;
    }

    @Value
    @AllArgsConstructor
    private static class RequestInfo {
        private final String action;
        private final String relativeArn;
        private final Optional<DestinationIPInfo> destinationIPInfoObject;
        private final Optional<ImmutableSet<String>> placementTags;

        public RequestInfo(String action, String relativeArn) {
            this(action, relativeArn, Optional.empty(), Optional.empty());
        }
    }

    private interface RequestInfoFunction<T> {
        RequestInfo getRequestInfo(String action, T request);
    }
}
