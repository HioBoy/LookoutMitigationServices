package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.ldaputils.LdapProvider;
import com.google.common.collect.ImmutableList;

/**
 * Helper to identify which POPs are Blackwatch capable.
 * Note - this helper only tells us if a POP has Blackwatch hosts built or not - it doesn't distinguish between whether Blackwatch is live or not.
 */
public class BlackwatchLocationsHelper {
    private static final Log LOG = LogFactory.getLog(BlackwatchLocationsHelper.class);
    
    // Constants to help performing a LDAP poll. 
    private static final String BASE_DISTINGUISHED_NAME = "ou=systems,ou=infrastructure,o=amazon.com";
    private static final String HOSTCLASS_LDAP_NAME = "amznDiscoHostClass";
    
    // For fetching the agent names from the hostclasses via LDAP
    private static final String HOSTCLASS_START_STR = "AWS-EDGE-";
    private static final String HOSTCLASS_END_STR = "-BW";
    
    // Configurations for attempts on failures.
    private static final int MAX_LDAP_ATTEMPTS = 3;
    private static final int SLEEP_BETWEEN_ATTEMPTS_IN_MILLIS = 500;
    
    private final LdapProvider ldapProvider;
    
    public BlackwatchLocationsHelper(@Nonnull LdapProvider ldapProvider) {
        Validate.notNull(ldapProvider);
        this.ldapProvider = ldapProvider;
    }
    
    /**
     * Returns whether the given POP can be considered a Blackwatch POP.
     * A POP is considered a Blackwatch POP if it has hosts built for that POP.
     * @param popName POP which needs to be checked for any built Blackwatch hosts.
     * @return true if this POP has Blackwatch hosts built for it, false otherwise.
     */
    public boolean isBlackwatchPOP(@Nonnull String popName) {
        Validate.notEmpty(popName);
        
        List<String> serversList = new ArrayList<String>();
        
        String hostclass = createBWHostclassForPOP(popName);
        
        int numAttempts = 0;
        while (numAttempts++ < MAX_LDAP_ATTEMPTS) {
            try {
                List<Map<String, List<Object>>> ldapResult = ldapProvider.search(BASE_DISTINGUISHED_NAME, "(" + HOSTCLASS_LDAP_NAME + "=" + hostclass + 
                                                                                 ")", Integer.MAX_VALUE, ImmutableList.of("cn"));
                serversList = ldapProvider.toSimpleList(ldapResult, "cn");
                break;
            } catch (Exception ex) {
                LOG.warn("Unable to poll LDAP for hostclass:" + hostclass, ex);
                
                if (numAttempts < MAX_LDAP_ATTEMPTS) {
                    try {
                        Thread.sleep(SLEEP_BETWEEN_ATTEMPTS_IN_MILLIS);
                    } catch (InterruptedException ignored) {}
                } else {
                	String msg = "Unable to poll LDAP to find if Blackwatch has hosts built in pop: " + popName + " after: " + numAttempts + " number of attempts";
                	LOG.error(msg);
                	throw new RuntimeException(msg);
                }
            }
        }
        
        if (!serversList.isEmpty()) {
            return true;
        }
        return false;
    }
    
    /**
     * Helper to create the Blackwatch hostclass name for the pop passed as input. Protected for unit tests.
     * @param popName POP for whom we need to create the Blackwatch hostclass name.
     * @return String representing the Blackwatch hostclass name for the input POP.
     */
    protected String createBWHostclassForPOP(String popName) {
        String hostclass = HOSTCLASS_START_STR + popName.toUpperCase() + HOSTCLASS_END_STR;
        
        // Include an edge-case for the BlackWatch gamma IAD5 host class so we can grab the right agent hosts
        if (popName.toUpperCase().equals("G-IAD5")) {
            hostclass = HOSTCLASS_START_STR + "G-IAD" + HOSTCLASS_END_STR;
        }
        return hostclass;
    }
}
