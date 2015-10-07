package com.amazon.lookout.mitigation.service.iam.setup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazonaws.services.identitymanagement.model.GetUserRequest;
import com.amazonaws.services.identitymanagement.model.ListUsersRequest;
import com.amazonaws.services.identitymanagement.model.ListUsersResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.google.common.collect.ImmutableSet;

public class VerifyIAMUsers extends IAMUserUtilsBase {
    private static final ImmutableSet<String> ignoreUsers = 
            ImmutableSet.of(
                    "blackwatch.host.status.updator.us-west-1.prod", "BlackWatchAgentS3ConfigDownloader", 
                    "LookoutMitigationServiceS3ConfigUploader", "mitigation.service.us-west-1.prod",
                    "s3IntegrationTest");
    
    public static void main(String args[]) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.WARN);
        
        if (args.length != 1) {
            System.err.println("Usage: java " + VerifyIAMUsers.class + " <domain>");
            System.exit(-1);
        }
        
        VerifyIAMUsers verifyIAMUsers = new VerifyIAMUsers(args[0]);
        boolean hadErrors = verifyIAMUsers.verify();
        if (hadErrors) {
            System.out.println("Verify had errors");
            System.exit(-1);
        }
    }
    
    private VerifyIAMUsers(String domain) {
        super(domain);
    }
    
    public boolean verify() {
        boolean hadError = false;
        
        hadError = checkForUnlistedUsers() || hadError;
        
        hadError = verifyCommonPolicies() || hadError;
        System.out.println();
        
        for (Map.Entry<String, Collection<String>> entry : userToPoliciesMap.asMap().entrySet()) {
            hadError = verifyUser(entry.getKey(), new HashSet<>(entry.getValue())) || hadError;
            System.out.println();
        }
        
        return hadError;
    }

    private boolean verifyCommonPolicies() {
        Map<String, AttachedPolicy> storedPolicies = getStoredPolicies();

        Set<String> matchingPolicies = new HashSet<>();
        Set<String> missingPolicies = new HashSet<>();
        Map<String, AttachedPolicy> mismatchedPolicies = new HashMap<>();
        for (Map.Entry<String, PolicyDescription> entry : policies.entrySet()) {
            String policyName = entry.getKey();
            AttachedPolicy storedPolicy = storedPolicies.get(entry.getKey());
            if (storedPolicy == null) {
                missingPolicies.add(policyName);
            } else if( !entry.getValue().policyEquals(iamClient, storedPolicy)) {
                mismatchedPolicies.put(policyName, storedPolicy);
            } else {
                matchingPolicies.add(policyName);
            }
        }
        
        System.out.println("Common Policies:");
        System.out.println("Matching policies  : " + matchingPolicies);
        System.out.println("Mismatched policies: " + mismatchedPolicies.keySet());
        System.out.println("Missing policies   : " + missingPolicies);
        
        if (mismatchedPolicies.isEmpty() && missingPolicies.isEmpty()) {
            return false;
        }
    
        System.out.println("Details:");
        
        if (!mismatchedPolicies.isEmpty()) {
            System.out.println("  Mismatched Policies:");
            for (Map.Entry<String, AttachedPolicy> entry : mismatchedPolicies.entrySet()) {
                String policyName = entry.getKey();
                AttachedPolicy storedPolicy = entry.getValue();
                
                PolicyDescription expected = policies.get(policyName);
                System.out.println("    Policy " + policyName + "(" + storedPolicy.getPolicySource() +"):");
                System.out.println("      Expected:");
                System.out.println(expected.getPolicyDocument());
                System.out.println("      Found:");
                System.out.println(storedPolicy.getFormattedPolicyDocument());
            }
        }
        
        if (!missingPolicies.isEmpty()) {
            System.out.println("  Missing Policies:");
            for (String policyName : missingPolicies) {
                PolicyDescription expected = policies.get(policyName);
                System.out.println("    Policy " + expected.getPolicyName() + ":");
                System.out.println(expected.getPolicyDocument());
            }
        }
        
        return true;
    }
    
    private boolean verifyUser(String userName, Set<String> expectedPolicies) {
        try {
            iamClient.getUser(new GetUserRequest().withUserName(userName)).getUser();
        } catch (NoSuchEntityException e) {
            System.out.println("User " + userName + " does not exist");
            System.out.println();
            return true;
        }
        
        System.out.println("User " + userName + ":");
        
        Map<String, List<StoredPolicy>> storedPolicies = getStoredPolicies(userName);
        
        Map<String, List<StoredPolicy>> unlistedPolicies = storedPolicies.entrySet().stream()
                .filter(e -> !expectedPolicies.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        
        Set<String> missingPolicies = new HashSet<>();
        Map<String, List<StoredPolicy>> matchingPolicies = new HashMap<>();
        Map<String, List<StoredPolicy>> mismatchedPolicies = new HashMap<>();
        
        for (String policyName : expectedPolicies) {
           List<StoredPolicy> storedPoliciesForName = storedPolicies.get(policyName);
           if (storedPoliciesForName == null) {
               missingPolicies.add(policyName);
           } else {
               List<StoredPolicy> mismatchedForName = new ArrayList<>();
               List<StoredPolicy> matchedForName = new ArrayList<>();
               
               for (StoredPolicy storedPolicy : storedPoliciesForName) {
                   PolicyDescription policyDescription = policies.get(policyName);
                   if (!policyDescription.policyEquals(iamClient, storedPolicy)) {
                       mismatchedForName.add(storedPolicy);
                   } else {
                       matchedForName.add(storedPolicy);
                   }
               }
               
               if (!mismatchedForName.isEmpty()) {
                   mismatchedPolicies.put(policyName, mismatchedForName);
               }
               
               if (!matchedForName.isEmpty()) {
                   matchingPolicies.put(policyName, matchedForName);
               }
           }
        }
        
        System.out.println("Matching policies  : " + shortFormatPoliciesMap(matchingPolicies));
        System.out.println("Mismatched policies: " + shortFormatPoliciesMap(mismatchedPolicies));
        System.out.println("Unlisted policies  : " + shortFormatPoliciesMap(unlistedPolicies));
        System.out.println("Missing policies   : " + missingPolicies);
        
        if (unlistedPolicies.isEmpty() && mismatchedPolicies.isEmpty() && missingPolicies.isEmpty()) {
            return false;
        }
    
        System.out.println("Details:");
        
        if (!unlistedPolicies.isEmpty()) {
            System.out.println("  Unlisted Policies:");
            for (Map.Entry<String, List<StoredPolicy>> entry : unlistedPolicies.entrySet()) {
                String policyName = entry.getKey();
                for (StoredPolicy storedPolicy : entry.getValue() ) {
                    System.out.println("    Policy " + policyName + "(" + storedPolicy.getPolicySource() +"):");
                    System.out.println(storedPolicy.getFormattedPolicyDocument());                    
                }
            }
        }
        
        if (!mismatchedPolicies.isEmpty()) {
            System.out.println("  Mismatched Policies:");
            for (Map.Entry<String, List<StoredPolicy>> entry : mismatchedPolicies.entrySet()) {
                String policyName = entry.getKey();
                PolicyDescription expected = policies.get(policyName);
                for (StoredPolicy storedPolicy : entry.getValue() ) {
                    System.out.println("    Policy " + policyName + "(" + storedPolicy.getPolicySource() +"):");
                    System.out.println("      Expected:");
                    System.out.println(expected.getPolicyDocument());
                    System.out.println("      Found:");
                    System.out.println(storedPolicy.getFormattedPolicyDocument());                    
                }
            }
        }
        
        if (!missingPolicies.isEmpty()) {
            System.out.println("  Missing Policies:");
            for (String policyName : missingPolicies) {
                PolicyDescription expected = policies.get(policyName);
                System.out.println("    Policy " + expected.getPolicyName() + ":");
                System.out.println(expected.getPolicyDocument());
            }
        }
        
        return true;
    }
    
    private static String policiesTypes(List<StoredPolicy> policies) {
        return policies.stream().map(p -> p.getPolicySource().name()).collect(Collectors.joining(", ", "(", ")"));
    }

    private static String shortFormatPoliciesMap(Map<String, List<StoredPolicy>> policies) {
        return policies.entrySet().stream().map(e -> e.getKey() + " " + policiesTypes(e.getValue())).collect(Collectors.joining(" "));
    }
    
    private boolean checkForUnlistedUsers() {
        List<String> unrecognizedUsers = new ArrayList<String>();
        String marker = null;
        do {
            ListUsersResult result = iamClient.listUsers(new ListUsersRequest().withMarker(marker));
            result.getUsers().stream()
                .map(user -> user.getUserName())
                .filter(name -> !(userToPoliciesMap.containsKey(name) || ignoreUsers.contains(name)))
                .forEach(name -> unrecognizedUsers.add(name));
            
            marker = result.getMarker();
        } while (marker != null);
        
        if (!unrecognizedUsers.isEmpty()) {
            System.out.println("Unrecognized users: " + unrecognizedUsers);
            System.out.println();
        }
        
        return !unrecognizedUsers.isEmpty();
    }
    
    
}
