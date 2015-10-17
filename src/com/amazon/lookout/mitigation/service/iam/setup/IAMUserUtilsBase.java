package com.amazon.lookout.mitigation.service.iam.setup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import amazon.odin.awsauth.OdinAWSCredentialsProvider;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.ListAttachedUserPoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListAttachedUserPoliciesResult;
import com.amazonaws.services.identitymanagement.model.ListPoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListPoliciesResult;
import com.amazonaws.services.identitymanagement.model.ListUserPoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListUserPoliciesResult;
import com.amazonaws.services.identitymanagement.model.PolicyScopeType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class IAMUserUtilsBase {
    private static final String OLD_CREDENTIALS_BASE = "com.amazon.lookout.lookoutmitigationservice.account.access";
    private static final String NEW_CREDENTIALS_BASE = "com.amazon.lookout.lookoutmitigationservice.iam";
    
    private static final ImmutableMap<String, PolicyDescription> basePolicies;
    
    static {
        List<PolicyDescription> policiesList = new ArrayList<>(); 
        policiesList.add(new PolicyDescription("AnyTemplate-AnyService-ReadNWrite-Allow", "anytemplate-anyservice-read-write.policy"));
        policiesList.add(new PolicyDescription("AnyTemplate-Route53Service-ReadNWrite-Allow", "anytemplate-route53-read-write.policy"));
        policiesList.add(new PolicyDescription("CountModeTemplate-Route53Service-ReadNWrite-Allow", "countmodetemplate-route53-read-write.policy"));
        policiesList.add(new PolicyDescription("BlackWatchPOP-EdgeCustomer-ReadNWrite-Allow", "blackwatchpop-edge-read-write.policy"));
        policiesList.add(new PolicyDescription("BlackWatchClusterTemplate-EC2Service-ReadNWrite-Allow", "countmodetemplate-route53-read-write.policy"));
        policiesList.add(new PolicyDescription("AnyTemplate-Blackhole-ReadNWrite-Allow", "anytemplate-blackhole-read-write.policy"));
        policiesList.add(new PolicyDescription("AnyAction-BlackholeDeviceMetadata-Allow", "blackholedevice-metadata-anyaction.policy"));
        policiesList.add(new PolicyDescription("AnyAction-TransitProviderMetadata-Allow", "transitprovider-metadata-anyaction.policy"));
        
        basePolicies = ImmutableMap.<String, PolicyDescription>copyOf(
                policiesList.stream().collect(Collectors.toMap(p -> p.getPolicyName(), p -> p)));
    }
    
    private static final ImmutableMultimap<String, String> baseUserToPoliciesMap;
    
    static {
        ImmutableMultimap.Builder<String, String> mapBuilder = ImmutableMultimap.builder();
        
        mapBuilder.put("LookoutMitigationServiceExplorer_User", "AnyTemplate-AnyService-ReadNWrite-Allow");
        
        mapBuilder.putAll("LookoutMitigationServiceUI_User", 
                "AnyTemplate-Route53Service-ReadNWrite-Allow",
                "AnyTemplate-Blackhole-ReadNWrite-Allow",
                "AnyAction-BlackholeDeviceMetadata-Allow",
                "AnyAction-TransitProviderMetadata-Allow");
        
        mapBuilder.put("LookoutMitigationServiceMonitor_User", "CountModeTemplate-Route53Service-ReadNWrite-Allow");
        
        mapBuilder.put("LookoutBlackWatchMitigationServiceCLI_User", "BlackWatchPOP-EdgeCustomer-ReadNWrite-Allow");
        
        mapBuilder.put("LookoutMitigationServiceBlackWatchCLI_User", "BlackWatchClusterTemplate-EC2Service-ReadNWrite-Allow");
        
        mapBuilder.putAll("LookoutMitigationServiceCLI_User",
                "AnyTemplate-Blackhole-ReadNWrite-Allow", 
                "AnyAction-BlackholeDeviceMetadata-Allow",
                "AnyAction-TransitProviderMetadata-Allow");
        
        baseUserToPoliciesMap = mapBuilder.build();
        
        if (!basePolicies.keySet().containsAll(baseUserToPoliciesMap.values())) {
            throw new IllegalArgumentException("User to policies map refers to unknown policies " + 
                    baseUserToPoliciesMap.values().stream().filter(p -> !basePolicies.keySet().contains(p)).collect(Collectors.joining(", ")));
        }
        
        if (!(new HashSet<>(baseUserToPoliciesMap.values()).equals(basePolicies.keySet()))) {
            Set<String> unusedPolicies = basePolicies.keySet().stream()
                    .filter(p -> !baseUserToPoliciesMap.values().contains(p))
                    .collect(Collectors.toSet());
            
            throw new IllegalArgumentException("Unused policies: " + unusedPolicies);
        }
    }
    
    /**
     * Unfortunately we're not fully consistent about names between domains. This contains the account the
     * mappings by domain from the names in baseUserToPoliciesMap to the name for that domain. Entries
     * not in the map are not renamed. The map is: domain -> { base name -> actual name } 
     */
    private static final ImmutableMap<String, ImmutableMap<String, String>> accountRenamesByDomain;
    
    static {
        ImmutableMap.Builder<String, ImmutableMap<String, String>> builder = ImmutableMap.builder();
        
        builder.put("beta", ImmutableMap.of(
                "LookoutMitigationServiceMonitor_User", "LookoutMitigationServiceExternalMonitor_User",
                "LookoutBlackWatchMitigationServiceCLI_User", "BlackWatchMitigationServiceCli_User"));
        
        builder.put("gamma", ImmutableMap.of(
                "LookoutMitigationServiceMonitor_User", "LookoutMitigationServiceExternalMonitor_User",
                "LookoutBlackWatchMitigationServiceCLI_User", "BlackWatchMitigationServiceAnyDeviceReadNWriteGamma_User"));
        
        accountRenamesByDomain = builder.build();
    }
    
    /**
     * Unfortunately we're not fully consistent about names between domains. This contains the account the
     * mappings by domain and user from the names in baseUserToPoliciesMap to the name for that domain and user. 
     * Entries not in the map are not renamed. The map is: domain -> { base policy name -> actual name }. 
     */
    private static final ImmutableMap<String, ImmutableMap<String, String>> policyRenamesByDomain;
    
    static {
        ImmutableMap.Builder<String, ImmutableMap<String, String>> builder = ImmutableMap.builder();
        
        ImmutableMap.Builder<String, String> betaMapBuilder = ImmutableMap.builder();
        betaMapBuilder.put("BlackWatchPOP-EdgeCustomer-ReadNWrite-Allow", "BlackWatchPOP_EdgeCustomer-Edge-ReadNWrite-Allow");
        builder.put("beta", betaMapBuilder.build());
        
        ImmutableMap.Builder<String, String> gammaMapBuilder = ImmutableMap.builder();
        gammaMapBuilder.put("BlackWatchPOP-EdgeCustomer-ReadNWrite-Allow", "Blackwatch-MitigationService-ReadNWrite-Allow");
        builder.put("gamma", gammaMapBuilder.build());
        
        policyRenamesByDomain = builder.build();
    }
    
    private static AmazonIdentityManagement getClient(String domain) {
        AmazonIdentityManagement iamClient;
        String credentials;
        String endpoint;
        switch(domain) {
        case "beta":
            credentials = OLD_CREDENTIALS_BASE + ".beta";
            endpoint = "https://aws-iams.integ.amazon.com";
            break;
        case "gamma":
            credentials = OLD_CREDENTIALS_BASE + ".gamma";
            endpoint = "https://aws-iams-gamma.amazon.com";
            break;
        default:
            credentials = NEW_CREDENTIALS_BASE + "." + domain;
            endpoint = "https://iam.amazonaws.com";
            break;
        }
        AWSCredentialsProvider credentialsProvider = new OdinAWSCredentialsProvider(credentials);
        iamClient = new AmazonIdentityManagementClient(credentialsProvider);
        iamClient.setEndpoint(endpoint);
        return iamClient;
    }
    
    private static ImmutableMap<String, PolicyDescription> buildPoliciesMap(String domain) {
        ImmutableMap<String, String> policyNameRemappings = policyRenamesByDomain.get(domain);
        if (policyNameRemappings == null) return basePolicies;
        
        ImmutableMap.Builder<String, PolicyDescription> builder = ImmutableMap.builder();
        
        for (Map.Entry<String, PolicyDescription> entry : basePolicies.entrySet()) {
            String name = policyNameRemappings.getOrDefault(entry.getKey(), entry.getKey());
            builder.put(name, entry.getValue());
        }
        
        return builder.build();
    }
    
    private static ImmutableMultimap<String, String> buildUserToPoliciesMapMap(String domain) {
        ImmutableMap<String, String> policyNameRemappings = policyRenamesByDomain.get(domain);
        Map<String, String> accountNameRemappings = accountRenamesByDomain.getOrDefault(domain, ImmutableMap.of());
        
        ImmutableMultimap.Builder<String, String> mapBuilder = ImmutableMultimap.builder();
        for (Entry<String, Collection<String>> entry : baseUserToPoliciesMap.asMap().entrySet()) {
            String name = accountNameRemappings.getOrDefault(entry.getKey(), entry.getKey());
            
            if (policyNameRemappings == null) {
                mapBuilder.putAll(name, entry.getValue());
            } else {
                List<String> policies = entry.getValue().stream()
                            .map(m -> policyNameRemappings.getOrDefault(m, m))
                            .collect(Collectors.toList());
                mapBuilder.putAll(name, ImmutableList.copyOf(policies));
            }
        }
        
        return mapBuilder.build();
    }
    
    protected IAMUserUtilsBase(String domain) {
        iamClient = getClient(domain);
        policies = buildPoliciesMap(domain);
        userToPoliciesMap = buildUserToPoliciesMapMap(domain);
    }
    
    protected final AmazonIdentityManagement iamClient;
    
    protected final ImmutableMap<String, PolicyDescription> policies;
    
    protected final ImmutableMultimap<String, String> userToPoliciesMap;
    
    protected Map<String, AttachedPolicy> getStoredPolicies() {
        Map<String, AttachedPolicy> result = new HashMap<>();
        String marker = null;
        do {
            ListPoliciesResult policiesResult = iamClient.listPolicies(new ListPoliciesRequest().withScope(PolicyScopeType.Local));
            policiesResult.getPolicies().forEach(p -> result.put(
                    p.getPolicyName(), new AttachedPolicy(iamClient, p.getArn(), p.getPolicyName())));
            marker = policiesResult.getMarker();
        } while (marker != null);
        return result;
    }
    
    /**
     * Get the policies from IAM. 
     * 
     * @param userName
     * @param policiesToFetch
     * @return a map from the policy name to the policy document stored in IAM.
     */
    protected Map<String, List<StoredPolicy>> getStoredPolicies(String userName) {
        Map<String, List<StoredPolicy>> policies = new HashMap<>();
        
        String marker = null;
        do {
            ListAttachedUserPoliciesResult policiesResult = 
                    iamClient.listAttachedUserPolicies(new ListAttachedUserPoliciesRequest().withUserName(userName).withMarker(marker));
            policiesResult.getAttachedPolicies().forEach(
                    policy -> policies.computeIfAbsent(policy.getPolicyName(), p -> new ArrayList<>())
                                      .add(new AttachedPolicy(iamClient, policy.getPolicyArn(), policy.getPolicyName())));
            marker = policiesResult.getMarker();
        } while (marker != null);
        
        marker = null;
        do {
            ListUserPoliciesResult policiesResult = 
                    iamClient.listUserPolicies(new ListUserPoliciesRequest().withUserName(userName).withMarker(marker));
            policiesResult.getPolicyNames().forEach(
                    name -> policies.computeIfAbsent(name, p -> new ArrayList<>())
                                    .add(new EmbeddedPolicy(iamClient, userName, name)));
            marker = policiesResult.getMarker();
        } while (marker != null);
        
        return policies;
    }
    
    protected boolean policyEquals(StoredPolicy policy1, StoredPolicy policy2) {
        return policy1.getPolicyDocument().equals(policy2.getPolicyDocument());
    }
}
