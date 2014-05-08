package com.amazon.lookout.mitigation.service.interceptors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.reflect.InvocationException;
import com.amazon.coral.reflect.InvocationInterceptor;
import com.amazon.coral.reflect.Invoker;
import com.amazon.coral.service.Activity;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.helpers.AWSUserGroupBasedAuthorizer;

/**
 * ActivityAuthorizationInterceptor is configured as a coral global interceptor (https://w.amazon.com/index.php/Coral/Cookbook/HowToUseGlobalInterceptors)
 * It intercepts requests to all activities in LookoutMitigationService and authorizes them as per the configuration passed to the interceptor.
 * 
 * This interceptor must be in the service-chain after the AuthenticationHandler - to ensure the AWS_USER_PRINCIPAL has been populated.
 * Also, by doing so, we are sure that this request is from a customer with valid credentials (awsAccessKey + awsSecretKey) - so 
 * the Activities can only focus on checking if this customer is authorized to perform the requested action.
 * 
 * This interceptor passes the call to the Activity and before returning back checks to ensure that we have performed an Authorization check
 * on this call - regardless if it was a failure or success.
 * 
 */
public class ActivityAuthorizationInterceptor implements InvocationInterceptor {
    
    private static final Log LOG = LogFactory.getLog(ActivityAuthorizationInterceptor.class);

    @Override
    public Object intercept(Invoker invoker, Object target, 
                            Object... parameters) throws InvocationException {
        try {
            // First process the service call
            return invoker.invoke( target, parameters );
        } finally {
            // After the service call has been processed, ensure that we performed an authorization check on the call.
            Activity activity = (Activity) target;
            Identity identity = activity.getIdentity();
            if (identity.getAttribute(AWSUserGroupBasedAuthorizer.AUTHORIZED_FLAG_KEY) == null) {
                LOG.warn("[UNAUTHORIZED_EXCEPTION] All LookoutMitigationService calls must be authorized. No " + AWSUserGroupBasedAuthorizer.AUTHORIZED_FLAG_KEY +
                          " flag found for request with target: " + target + " parameters: " + parameters);
                throw new InternalServerError500("Unable to authorize request");
            }
        }
    }
}
