package com.amazon.lookout.mitigation.service.authorization;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import aws.auth.client.config.Configuration;
import com.amazon.aspen.entity.Policy;
import com.amazon.coral.security.AccessDeniedException;
import com.amazon.coral.service.AbstractAwsAuthorizationStrategy;
import com.amazon.coral.service.AuthorizationInfo;
import com.amazon.coral.service.BasicAuthorizationInfo;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
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
 * resource: arn:<partition>:<vendor>:<region>:<namespace>:<servicename>-<devicename>
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
    private String region;
    private static final String VENDOR = "lookout";
    private static final String NAMESPACE = "";
    private static final String SEPARATOR = "-";
    
    // Some APIs do not concern with MitigationTemplate. In such cases we use ANY_TEMPLATE constant in the ARN. 
    protected static final String ANY_TEMPLATE = "ANY_TEMPLATE";
    
    public AuthorizationStrategy(Configuration arcConfig, String region) {
        super(arcConfig);
        this.region = region;
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
            throws Throwable {
        List<AuthorizationInfo> authInfoList = new LinkedList<AuthorizationInfo>();
        String actionName = generateActionName(context.getOperation().toString(), request);
        String resourceName = generateResourceName(request);
        LOG.debug("Action: " + actionName + " ; " + "Resource (ARN): " + resourceName);        
        if (resourceName == null) {            
            throw new AccessDeniedException("LookoutMitigationService did not recognize the incoming request: " + request);
        }
        
        BasicAuthorizationInfo authorizationInfo = new BasicAuthorizationInfo();
        // Action that need to be authorized
        authorizationInfo.setAction(actionName);
        // Resource that is guarded
        authorizationInfo.setResource(resourceName);
        // Principal identifier of the resource owner
        // associated with this authorization call
        authorizationInfo.setResourceOwner(context.getIdentity().getAttribute(Identity.AWS_ACCOUNT));
        // Any custom policies to include in the authorization check
        authorizationInfo.setPolicies(Collections.<Policy>emptyList());

        authInfoList.add(authorizationInfo);

        return authInfoList;
    }
    
    private boolean isMitigationModificationRequest(final Object request) {
        return (request instanceof MitigationModificationRequest); 
    }    
    
    private boolean isGetRequestStatusRequest(final Object request) {
        return (request instanceof GetRequestStatusRequest);
    }
    
    private boolean isListActiveMitigationsForServiceRequest(final Object request) {
        return (request instanceof ListActiveMitigationsForServiceRequest);
    }
        
    /* 
     * Generate Amazon Resource Name (ARN) looking inside the Request, with the following structure
     * arn:<partition>:<vendor>:<region>:<namespace>:<relative-id>, as described at:
     * https://w.amazon.com/index.php/AWS/Common/Naming/Identifiers#Canonical_Names 
     */
    protected String generateResourceName(final Object request) {
        StringBuilder arnBuilder = new StringBuilder();
        /**
         * Note: Currently, all serviceName+deviceName combinations are equally accessible from
         * all regions where LookoutMitigationService are executing.   
         */
        arnBuilder.append("arn")
                  .append(":")
                  .append(PARTITION)
                  .append(":")
                  .append(VENDOR)
                  .append(":")
                  .append(region)
                  .append(":")
                  .append(NAMESPACE)
                  .append(":");
        
        boolean recognizedRequest = true;
        String deviceName = null;
        String serviceName = null;
        String mitigationTemplate = null;
        
        // TODO: Add a helper class to retrieve deviceName and serviceName for all request types,
        // and use that here.
        
        // All MitigationModificationRequests share authorization policy
        if (isMitigationModificationRequest(request)) {
            // create relative-id
            MitigationModificationRequest mitigationModificationRequest = (MitigationModificationRequest) request;
            mitigationTemplate = mitigationModificationRequest.getMitigationTemplate();
            serviceName = mitigationModificationRequest.getServiceName();
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            if (deviceNameAndScope == null) {
                return null;
            }
            deviceName = deviceNameAndScope.getDeviceName().name();
        } else if (isGetRequestStatusRequest(request)) {
            GetRequestStatusRequest getRequestStatusRequest = (GetRequestStatusRequest) request;
            mitigationTemplate = null;
            serviceName = getRequestStatusRequest.getServiceName();
            deviceName = getRequestStatusRequest.getDeviceName();
        } else if (isListActiveMitigationsForServiceRequest(request)) {
            ListActiveMitigationsForServiceRequest listMitigationsRequest = (ListActiveMitigationsForServiceRequest) request;                       
            mitigationTemplate = null;
            serviceName = listMitigationsRequest.getServiceName();
            // deviceName is not required and may be null
            deviceName = listMitigationsRequest.getDeviceName();
        } else {
            recognizedRequest = false;
        }
        
        if (recognizedRequest) {            
            String relativeId = getRelativeId(mitigationTemplate, serviceName, deviceName);            
            arnBuilder.append(relativeId);
            return arnBuilder.toString();
        }
             
        return null;
    }
    
    /**
     * serviceName+deviceName+mitigationTemplate combination is included in the relative identifier as: 
     * mitigationTemplate/serviceName-deviceName if mitigationTemplate is not null, else as: serviceName-deviceName.
     *
     * mitigationTemplate may be non-null to selectively authorize MitigationModificationRequests for some 
     * serviceName+deviceName combinations. E.g., different sets of users may be authorized for applying
     * ratelimit and count filters on routers.
     */
    protected String getRelativeId(String mitigationTemplate, final String serviceName, String deviceName) {
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
     * Generate action name with the following structure:
     * action: <vendor>:read-<operationname> or <vendor>:write-<operationname>
     */
    protected String generateActionName(final String operationName, final Object request) {
        StringBuilder actionName = new StringBuilder();
        actionName.append(VENDOR)
                  .append(":");
        String prefix = "";
        if (isMitigationModificationRequest(request) /*|| isGetRequestStatus(request)*/) {
            prefix = WRITE_OPERATION_PREFIX;
        } else {
            prefix = READ_OPERATION_PREFIX;
        }
        actionName.append(prefix)
                  .append(SEPARATOR)
                  .append(operationName);
        return actionName.toString();
    }
    
    @Override
    public String getStrategyName() {
        return this.getClass().getName();
    }
}
