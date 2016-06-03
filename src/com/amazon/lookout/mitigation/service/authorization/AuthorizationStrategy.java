package com.amazon.lookout.mitigation.service.authorization;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import lombok.Data;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import aws.auth.client.config.Configuration;

import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AbstractAwsAuthorizationStrategy;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.lookout.mitigation.service.CreateBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.CreateTransitProviderRequest;
import com.amazon.lookout.mitigation.service.GetBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.GetTransitProviderRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListBlackholeDevicesRequest;
import com.amazon.lookout.mitigation.service.ListTransitProvidersRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.UpdateTransitProviderRequest;
import com.amazon.lookout.mitigation.service.activity.GetLocationHostStatusActivity;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;

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
    private static final Log LOG = LogFactory.getLog(AuthorizationStrategy.class);
    /**
     * Some operations such as GetRequestStatus are not write operations but 
     * are only relevant when they follow write operations such as CreateMitigation,
     * are only authorized to those who have permissions for the latter write operations.
     * The action names for such operations would be generated with write_operation_prefix.
     */
    private static final String WRITE_OPERATION_PREFIX = "write";
    private static final String READ_OPERATION_PREFIX = "read";

    // Constants used for generating ARN
    private static final String PARTITION = "aws";
    private static final String VENDOR = "lookout";
    private static final String SEPARATOR = "-";

    // Some APIs do not concern with MitigationTemplate. In such cases we use ANY_TEMPLATE constant in the ARN. 
    protected static final String ANY_TEMPLATE = "ANY_TEMPLATE";

    private final String ownerAccountId;

    private final String arnPrefix;

    public AuthorizationStrategy(Configuration arcConfig, String region, String ownerAccountId) {
        super(arcConfig);
        Validate.notNull(arcConfig);
        Validate.notEmpty(region);
        Validate.notEmpty(ownerAccountId);
        this.ownerAccountId = ownerAccountId;
        this.arnPrefix = "arn:" + PARTITION + ":" + VENDOR + ":" + region + ":" + ownerAccountId + ":";
    }

    /**
     * (non-Javadoc)
     * @see {@link com.amazon.coral.service.AbstractAwsAuthorizationStrategy#getAuthorizationInfoList(com.amazon.coral.service.Context, java.lang.Object)
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
    public List<AuthorizationInfo> getAuthorizationInfoList(final Context context, final Object request) 
            throws AccessDeniedException
            {

        RequestInfo requestInfo = getRequestInfo(context.getOperation().toString(), request);
        if (requestInfo == null) {
            throw new RuntimeException("Failed getting request info for request " + request);
        }

        String resourceName = arnPrefix + requestInfo.getRelativeArn();
        LOG.debug("Action: " + requestInfo.getAction() + " ; " + "Resource (ARN): " + resourceName);        

        BasicAuthorizationInfo authorizationInfo = new BasicAuthorizationInfo();
        // Action that need to be authorized
        authorizationInfo.setAction(requestInfo.getAction());
        // Resource that is guarded
        authorizationInfo.setResource(resourceName);
        // Principal identifier of the resource owner
        // associated with this authorization call
        authorizationInfo.setResourceOwner(ownerAccountId);
        // Any custom policies to include in the authorization check
        authorizationInfo.setPolicies(Collections.emptyList());

        List<AuthorizationInfo> authInfoList = new LinkedList<AuthorizationInfo>();
        authInfoList.add(authorizationInfo);

        return authInfoList;
            }

    @Data
    static class RequestInfo {
        private final String action;
        private final String relativeArn;
    }

    interface RequestInfoFunction<T> {
        public RequestInfo getRequestInfo(String action, T request);
    }

    private static Map<Class<?>, RequestInfoFunction<Object>> requestInfoParsers;

    @SuppressWarnings("unchecked")
    private static <T> void addRequestInfoParser(Class<T> clazz, RequestInfoFunction<T> function) {
        if (requestInfoParsers.containsKey(clazz)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " was already registered");
        }
        requestInfoParsers.put(clazz, (RequestInfoFunction<Object>) function);
    }

    private static RequestInfo generateMitigationRequestInfo(
            String action, String prefix, String serviceName, String deviceName, String mitigationTemplate)
    {
        return new RequestInfo(
                generateActionName(action, prefix), 
                getMitigationRelativeId(serviceName, deviceName, mitigationTemplate));
    }

    private static RequestInfo generateHostStatusRequestInfo(String action, String prefix, String locationName)
    {
        return new RequestInfo(
                generateActionName(action, prefix),
                getLocationRelativeId(locationName));
    }

    private static RequestInfo generateMetadataRequestInfo(
            String action, String prefix, String metadataType)
    {
        Validate.notNull(metadataType);
        Validate.notNull(prefix);

        return new RequestInfo(
                generateActionName(action, prefix), 
                "metadata" + "/" + metadataType);
    }

    static {
        requestInfoParsers = new ConcurrentHashMap<>();

        // All MitigationModificationRequests share authorization policy
        addRequestInfoParser(
                MitigationModificationRequest.class,
                (action, request) -> {
                    String mitigationTemplate = request.getMitigationTemplate();
                    if (StringUtils.isEmpty(mitigationTemplate)) {
                        throw new AccessDeniedException("Missing mitigationTemplate");
                    }

                    DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
                    if (deviceNameAndScope == null) {
                        throw new AccessDeniedException("Unrecognized template " + mitigationTemplate);
                    }
                    String deviceName = deviceNameAndScope.getDeviceName().name();

                    String serviceName = request.getServiceName();
                    if (StringUtils.isEmpty(serviceName)) {
                        throw new AccessDeniedException("Missing serviceName");
                    }

                    return generateMitigationRequestInfo(action, WRITE_OPERATION_PREFIX, serviceName, deviceName, mitigationTemplate);
                });

        addRequestInfoParser(
                GetRequestStatusRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                ListActiveMitigationsForServiceRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                GetMitigationInfoRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                ReportInactiveLocationRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                GetMitigationDefinitionRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                GetMitigationHistoryRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                GetLocationDeploymentHistoryRequest.class, 
                (action, request) -> 
                generateMitigationRequestInfo(action, READ_OPERATION_PREFIX, request.getServiceName(), request.getDeviceName(), null));

        addRequestInfoParser(
                GetLocationHostStatusRequest.class, 
                (action, request) -> 
                generateHostStatusRequestInfo(action, READ_OPERATION_PREFIX, request.getLocation()));

        for (Class<?> clazz : new Class[]{ListBlackholeDevicesRequest.class, GetBlackholeDeviceRequest.class} ) {
            addRequestInfoParser(
                    clazz, (action, request) -> generateMetadataRequestInfo(action, READ_OPERATION_PREFIX, "blackhole-device"));
        }

        for (Class<?> clazz : new Class[]{CreateBlackholeDeviceRequest.class, UpdateBlackholeDeviceRequest.class} ) {
            addRequestInfoParser(
                    clazz, (action, request) -> generateMetadataRequestInfo(action, WRITE_OPERATION_PREFIX, "blackhole-device"));
        }

        for (Class<?> clazz : new Class[]{ListTransitProvidersRequest.class, GetTransitProviderRequest.class} ) {
            addRequestInfoParser(
                    clazz, (action, request) -> generateMetadataRequestInfo(action, READ_OPERATION_PREFIX, "transit-provider"));
        }

        for (Class<?> clazz : new Class[]{CreateTransitProviderRequest.class, UpdateTransitProviderRequest.class} ) {
            addRequestInfoParser(
                    clazz, (action, request) -> generateMetadataRequestInfo(action, WRITE_OPERATION_PREFIX, "transit-provider"));
        }
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
    static String getMitigationRelativeId(String serviceName, String deviceName, String mitigationTemplate) {
        if (deviceName == null) {
            /**
             * for some request types deviceName is not a required field. In those cases deviceName is 
             * set to ANY_DEVICE, granting authorization for all devices, or none.
             */
            deviceName = DeviceName.ANY_DEVICE.name();
        }

        if (mitigationTemplate == null) {
            mitigationTemplate = ANY_TEMPLATE;
        }
        StringBuilder relativeidBuilder = new StringBuilder();
        relativeidBuilder.append(mitigationTemplate)
        .append("/")        
        .append(serviceName)
        .append(SEPARATOR)
        .append(deviceName);

        return relativeidBuilder.toString();
    }

    /**
     * locationName is included in the relative identifier as: 
     * LOCATION/locationName
     *
     * locationName should be non-null
     */
    static String getLocationRelativeId(String locationName) {
        StringBuilder relativeidBuilder = new StringBuilder();
        relativeidBuilder.append("LOCATION")
        .append("/")        
        .append(locationName);

        return relativeidBuilder.toString();
    }    
    
    /**
     * Generate action name with the following structure:
     * action: <vendor>:prefix-<operationname>
     */
    static String generateActionName(final String operationName, String prefix) {
        Validate.notNull(operationName);
        Validate.notNull(prefix);

        StringBuilder actionName = new StringBuilder();
        actionName.append(VENDOR)
        .append(":")
        .append(prefix)
        .append(SEPARATOR)
        .append(operationName);
        return actionName.toString();
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getName();
    }
}
