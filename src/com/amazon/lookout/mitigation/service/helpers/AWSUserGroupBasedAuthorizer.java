package com.amazon.lookout.mitigation.service.helpers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import net.jcip.annotations.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.coral.service.Identity;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
//import com.amazon.lookout.mitigation.service.SeviceName;

/**
 * Lookout creates up to two IAM groups per ServiceName+DeviceName. The two IAM groups are used to grant write and read-only accesses. 
 * Lookout also creates an IAM user for each of the above IAM groups.  
 * Requests are signed by credentials of one of the above IAM users.
 * An IAM user can belong to more than one IAM groups, allowing it to perform operations on multiple ServiceName+DeviceName combinations.
 * 
 * For authorization, we maintain a mapping from group ids to clients and operations to clients.
 * These clients correspond with IAM groups (and so an IAM user can be mapped to multiple clients)
 */

@ThreadSafe
public class AWSUserGroupBasedAuthorizer {
    public static final String AUTHORIZED_FLAG_KEY = "Authorized";
    
    private final Map<String, String> groupIdToClientMap;
    
    private final Map<String, List<String>> operationToClientsMap;
    
    private final List<String> operationsToAuthWithWriteCredentials;
    
    // for clients with write privileges
    protected static final String CLIENT_SUFFIX = "Client";
    // for clients with only read only access
    protected static final String READ_ONLY_CLIENT_SUFFIX = "ReadOnlyClient";
    
    public static final String LOOKOUT_MITIGATION_SERVICE_EXPLORER = "LookoutMitigationServiceExplorer";
    
    // TODO: use enum class to get these values instead
    // public static final String ROUTE53_SERVICE_NAME = ServiceName.Route53;
    private static final String ROUTE53_SERVICE_NAME = "Route53";
    
    public static final String ROUTE53_POP_ROUTER_CLIENT = generateClientName(ROUTE53_SERVICE_NAME,
            DeviceName.POP_ROUTER.name(), false);
    public static final String ROUTE53_POP_ROUTER_READONLY_CLIENT = generateClientName(ROUTE53_SERVICE_NAME,
            DeviceName.POP_ROUTER.name(), true);
    public static final String ROUTE53_ANY_DEVICE_READONLY_CLIENT = generateClientName(ROUTE53_SERVICE_NAME,
            DeviceName.ANY_DEVICE.name(), true);
    
    private static final Log LOG = LogFactory.getLog(AWSUserGroupBasedAuthorizer.class);
    
    public AWSUserGroupBasedAuthorizer(@Nonnull Map<String, String> groupIdToClientMap,
                                       @Nonnull Map<String, List<String>> operationToClientsMap,
                                       @Nonnull List<String> operationsToAuthWithWriteCredentials) {
        Validate.notEmpty(groupIdToClientMap);
        this.groupIdToClientMap = groupIdToClientMap;

        Validate.notEmpty(operationToClientsMap);
        this.operationToClientsMap = operationToClientsMap;
        
        Validate.notNull(operationsToAuthWithWriteCredentials);
        this.operationsToAuthWithWriteCredentials = operationsToAuthWithWriteCredentials;
    }
    
    
    /**
     * Extracts groups that this user belongs too and returns only the groups we are aware of (based on the configuration)
     * @param identity
     * @return List of configured groups this user belongs to.
     */
    private List<String> extractValidUserGroups(Identity identity) {
        String commaSeparatedGroups = identity.getAttribute(Identity.AWS_USER_GROUPS);
        List<String> userGroups = new LinkedList<String>();
        if (commaSeparatedGroups != null) {
            for (String group : commaSeparatedGroups.split(",")) {
            	group = group.trim();
                if (groupIdToClientMap.containsKey(group)) {
                    userGroups.add(group);
                }
            }
        }
        return userGroups;
    }
    

    /**
     * Checks if the client sending this request is a valid client.
     * Basically invokes the extractValidUserGroups and returns true if we have at least 1 valid group in the list, else false.
     */
    protected boolean isValidClient(@Nonnull Identity identity) {
    	Validate.notNull(identity);
        List<String> groups = extractValidUserGroups(identity);
        if (groups != null && groups.size() > 0) {
            return true;
        }
        return false;
    }

    
    /**
     * Checks if the client is authorized to perform operation on the
     * serviceName+deviceName. All operations are permitted for any
     * serviceName+deviceName combinations for calls from
     * LookoutMitigationServiceExplorer
     */
    private boolean isClientAuthorizedForServiceAndDevice(Identity identity, String serviceName, String deviceName,
            String operation) {
        if (!isValidClient(identity) 
                || serviceName == null || deviceName == null || operation == null 
                || (!operationToClientsMap.containsKey(operation))) {
            LOG.warn("[UNAUTHORIZED_REQUEST] Request for operation: " + operation + " for service: " + serviceName + " on device: " + deviceName + " by an invalid client " + 
                     "with AccessKeyId: " + identity.getAttribute(Identity.AWS_ACCESS_KEY) + " userPrincipal: " + identity.getAttribute(Identity.AWS_USER_PRINCIPAL) +
                     " Groups: " + identity.getAttribute(Identity.AWS_USER_GROUPS));
            return false;
        }
        
        String expectedClientWithWritePermissions = generateClientName(serviceName, deviceName, false);
        String expectedClientWithReadOnlyPermissions = generateClientName(serviceName, deviceName, true);
        
        List<String> groups = extractValidUserGroups(identity);
        List<String> authorizedClients = operationToClientsMap.get(operation);
        
        for (String group : groups) {
            String client = groupIdToClientMap.get(group);
         
            // If the client belongs to the right serviceName+deviceName combination or is 
            // LookoutMitigationServiceExplorer then authorize request
            if (client.equals(expectedClientWithWritePermissions)
                    || ((!requiresWriteCredentials(operation)) && client.equals(expectedClientWithReadOnlyPermissions))
                    || client.equals(LOOKOUT_MITIGATION_SERVICE_EXPLORER)) {
                // Client is authorized for some operations for serviceName+deviceName
                // Now check if the client is authorized for the operation if this is
                // not a LOOKOUT_MITIGATION_SERVICE_EXPLORER
                if (client.equals(LOOKOUT_MITIGATION_SERVICE_EXPLORER) 
                        || authorizedClients.contains(client)) {
                    LOG.info("Client " + client + " authorized for operation " + operation + " for service: "
                            + serviceName + " on device: " + deviceName + " with AccessKeyId: "
                            + identity.getAttribute(Identity.AWS_ACCESS_KEY) + " userPrincipal: "
                            + identity.getAttribute(Identity.AWS_USER_PRINCIPAL) + " belonging to group: " + group);
                    return true;
                }
            }
        }
        
        LOG.warn("[UNAUTHORIZED_REQUEST] Unauthorized request for operation " + operation + " for service: " + serviceName + " on device: " + deviceName + 
                 " with AccessKeyId: " + identity.getAttribute(Identity.AWS_ACCESS_KEY) + " userPrincipal: " + identity.getAttribute(Identity.AWS_USER_PRINCIPAL) +
                 " Groups: " + groups);
        return false;
    }
    
    /**
     * Checks if the client is authorized to perform operation on the
     * serviceName+deviceName.
     */
    public boolean isClientAuthorized(Identity identity, String serviceName, String deviceName, String operation) {
        return isClientAuthorizedForServiceAndDevice(identity, serviceName, deviceName, operation);
    }

    /**
     * Checks if the client is authorized to perform operation on the
     * serviceName for any device.
     */
    public boolean isClientAuthorized(Identity identity, String serviceName, String operation) {
        return isClientAuthorizedForServiceAndDevice(identity, serviceName, DeviceName.ANY_DEVICE.name(), operation);
    }

    /**
     * Sets the authorized flag to indicate that we have authorized this service
     * call.
     */
    public void setAuthorizedFlag(Identity identity, Boolean flag) {
        if (identity != null && flag != null) {
            identity.setAttribute(AUTHORIZED_FLAG_KEY, flag.toString());
        }
    }
    
    private boolean requiresWriteCredentials(String operation) {
        // by default operations require read operations
        return (operationsToAuthWithWriteCredentials.contains(operation));
    }
    
    private static String generateClientName(String serviceName, String deviceName, boolean readOnly) {
        return (serviceName + deviceName + (readOnly ? READ_ONLY_CLIENT_SUFFIX : CLIENT_SUFFIX));
    }
}