package com.amazon.lookout.mitigation.service.iam.setup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazon.lookout.mitigation.service.iam.setup.StoredPolicy.PolicySource;
import com.amazonaws.services.identitymanagement.model.CreateUserRequest;
import com.amazonaws.services.identitymanagement.model.GetUserRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.google.common.collect.ImmutableSet;

public class CreateIAMUsers extends IAMUserUtilsBase {
    public static void main(String args[]) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        
        if (args.length != 1) {
            System.err.println("Usage: java " + CreateIAMUsers.class + " <domain>");
            System.exit(-1);
        }
        
        new CreateIAMUsers(args[0]).create();
    }
    
    private CreateIAMUsers(String domain) {
        super(domain);
    }

    public void create() {
        Map<String, AttachedPolicy> commonPolicies = getStoredPolicies();
        System.out.println("Creating/Updating common policies");
        createCommonPolicies(commonPolicies);
        
        for (Map.Entry<String, Collection<String>> entry : userToPoliciesMap.asMap().entrySet()) {
            System.out.println("Creating/Updating " + entry.getKey());
            createUser(entry.getKey(), entry.getValue(), commonPolicies);
            System.out.println();
        }
    }
    
    private void createCommonPolicies(Map<String, AttachedPolicy> commonPolicies) {
        
        for (Map.Entry<String, PolicyDescription> entry : policies.entrySet()) {
            createCommonPolicy(entry.getKey(), entry.getValue(), commonPolicies);
        }
    }
    
    private void createCommonPolicy(String policyName, PolicyDescription expected, Map<String, AttachedPolicy> commonPolicies) {
        AttachedPolicy storedPolicy = commonPolicies.get(policyName);
        String promptMessage;
        if (storedPolicy == null) {
            promptMessage = "Create Common Policy " + policyName;
        } else if( !expected.policyEquals(iamClient, storedPolicy)) {
            System.out.println("Policy " + policyName + " already exists but does not match:");
            System.out.println("  Expected:");
            System.out.println(expected.getPolicyDocument());
            System.out.println("  Found:");
            System.out.println(storedPolicy.getFormattedPolicyDocument());
            promptMessage = "Update Common Policy " + policyName;
        } else {
            // Nothing to change
            return;
        }
        
        if (!confirm(promptMessage)) {
            return;
        }
        
        if (storedPolicy == null) {
            storedPolicy = AttachedPolicy.createPolicy(iamClient, expected);
            commonPolicies.put(policyName, storedPolicy);
        } else {
            storedPolicy.updatePolicy(expected.getPolicyDocument());
        }
    }
    
    private void createUser(String userName, Collection<String> userPolicies,  Map<String, AttachedPolicy> commonPolicies) {
        try {
            iamClient.getUser(new GetUserRequest().withUserName(userName)).getUser();
        } catch (NoSuchEntityException e) {
            if (!confirm("User " + userName + " does not exist. Create")) {
                System.out.println("Skipping user " + userName);
                return;
            }
            iamClient.createUser(new CreateUserRequest(userName));
        }
        
        Map<String, List<StoredPolicy>> existingPoliciesForUser = getStoredPolicies(userName);
        for (String policy : userPolicies) {
            PolicyDescription expected = policies.get(policy);
            
            List<StoredPolicy> existingPolicies = existingPoliciesForUser.get(policy);
            if (existingPolicies == null) {
                if (commonPolicies.containsKey(policy)) {
                    if (confirm("Attach policy " + policy + " to user " + userName)) {
                        commonPolicies.get(policy).attach(userName);
                    } else {
                        System.out.println("Skipping policy " + policy + " for user " + userName);
                    }
                } else {
                    if (confirm("Create embedded policy " + policy + " for user " + userName)) {
                        EmbeddedPolicy.createPolicy(iamClient, userName, expected);
                    } else {
                        System.out.println("Skipping policy " + policy + " for user " + userName);
                    }
                }
            } else if (existingPolicies.size() == 1) {
                StoredPolicy storedPolicy = existingPolicies.get(0);
                if (storedPolicy instanceof EmbeddedPolicy) {
                    boolean updated = false;
                    boolean printedDiff = false;
                    if (commonPolicies.containsKey(policy)) {
                        AttachedPolicy commonPolicy = commonPolicies.get(policy);
                        if (policyEquals(storedPolicy, commonPolicy)) {
                            if (confirm("Convert embedded policy " + policy + " to matching attached policy for " + userName)) {
                                commonPolicies.get(policy).attach(userName);
                                ((EmbeddedPolicy) storedPolicy).removePolicy();
                                updated = true;
                            }
                        } else {
                            System.out.println("User " + userName + " has an embedded policy for " + policy + " that does not match");
                            System.out.println("the common policy with the same name.");
                            System.out.println("  Common:");
                            System.out.println(commonPolicy.getFormattedPolicyDocument());
                            System.out.println("  Embedded:");
                            System.out.println(storedPolicy.getFormattedPolicyDocument());
                            System.out.println("  Expected:");
                            System.out.println(expected.getPolicyDocument());
                            printedDiff = true;
                            if (confirm("Replace embedded policy with the common policy")) {
                                commonPolicies.get(policy).attach(userName);
                                ((EmbeddedPolicy) storedPolicy).removePolicy();
                                updated = true;
                            }
                        }
                    }
                    
                    if (!updated && !expected.policyEquals(iamClient, storedPolicy)) {
                        if (!printedDiff) {
                            System.out.println("Policy " + policy + " already exists but does not match:");
                            System.out.println("  Expected:");
                            System.out.println(expected.getPolicyDocument());
                            System.out.println("  Found:");
                            System.out.println(storedPolicy.getFormattedPolicyDocument());
                        }
                        if (confirm("Update Embedded Policy " + policy)) {
                            storedPolicy.updatePolicy(expected.getPolicyDocument());
                        } else {
                            System.out.println("Skipping policy " + policy + " for user " + userName);
                        }
                    }
                }
            } else if (existingPolicies.size() != 2) {
                throw new IllegalArgumentException("Only expected two possible sources for policies but found more for " + policy);
            } else {
                Set<PolicySource> sources = existingPolicies.stream().map(p -> p.getPolicySource()).collect(Collectors.toSet());
                if (!sources.equals(ImmutableSet.of(PolicySource.ATTACHED, PolicySource.EMBEDDED))) {
                    throw new IllegalArgumentException("Unexpected policy sources " + sources);
                }
                
                EmbeddedPolicy embeddedPolicy;
                AttachedPolicy attachedPolicy;
                if (existingPolicies.get(0) instanceof EmbeddedPolicy) {
                    embeddedPolicy = (EmbeddedPolicy) existingPolicies.get(0);
                    attachedPolicy = (AttachedPolicy) existingPolicies.get(1);
                } else {
                    embeddedPolicy = (EmbeddedPolicy) existingPolicies.get(1);
                    attachedPolicy = (AttachedPolicy) existingPolicies.get(0);
                }
                
                if (policyEquals(embeddedPolicy, attachedPolicy)) {
                    System.out.println("Policy " + policy + " has both attached and embedded versions");
                } else {
                    System.out.println("Policy " + policy + " has both attached and common versions but they do not match");
                    System.out.println("  Common:");
                    System.out.println(attachedPolicy.getFormattedPolicyDocument());
                    System.out.println("  Embedded:");
                    System.out.println(embeddedPolicy.getFormattedPolicyDocument());
                    System.out.println("  Expected:");
                    System.out.println(expected.getPolicyDocument());
                }
                
                if (confirm("Remove emedded version")) {
                    embeddedPolicy.removePolicy();
                } else {
                    System.out.println("Skipping policy " + policy + " for user " + userName);
                }
            }
        }
    }

    private static boolean confirm(String message) {
        System.out.println(message + " (Y/[N]): ");
        
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        try {
            for(;;) {
                int read;
                    read = System.in.read();
                if (read == -1 || read == '\n') break;
                responseBuffer.write(read);
            }
        } catch (IOException e) {
            System.err.println("Failed reading response.");
            e.printStackTrace();
            return false;
        }
        
        if (responseBuffer.size() == 0) {
            return false;
        }
        
        String response = new String(responseBuffer.toByteArray(), StandardCharsets.UTF_8).toUpperCase();
        if (response.equals("Y") || response.equals("YES")) {
            return true;
        } else {
            return false;
        }
    }
}

