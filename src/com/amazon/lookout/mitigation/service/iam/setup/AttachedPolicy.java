package com.amazon.lookout.mitigation.service.iam.setup;

import java.util.Optional;

import lombok.Getter;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.AttachUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreatePolicyVersionRequest;
import com.amazonaws.services.identitymanagement.model.GetPolicyVersionRequest;
import com.amazonaws.services.identitymanagement.model.ListPolicyVersionsRequest;
import com.amazonaws.services.identitymanagement.model.ListPolicyVersionsResult;
import com.amazonaws.services.identitymanagement.model.PolicyVersion;

public class AttachedPolicy extends StoredPolicy {
    public static AttachedPolicy createPolicy(AmazonIdentityManagement iamClient, PolicyDescription policy) {
        CreatePolicyResult createResult = iamClient.createPolicy(new CreatePolicyRequest()
            .withPolicyName(policy.getPolicyName())
            .withPolicyDocument(policy.getPolicyDocument()));
        return new AttachedPolicy(iamClient, createResult.getPolicy().getArn(), policy.getPolicyName());
    }
    
    @Getter
    private final String policyArn;
    
    private transient String activeVersion;
    
    public AttachedPolicy(AmazonIdentityManagement iamClient, String policyArn, String policyName) {
        super(iamClient, policyName, PolicySource.ATTACHED);
        this.policyArn = policyArn;
    }
    
    @Override
    public synchronized void updatePolicy(String newPolicy) {
        // Clear the active version first - even if we get an exception
        // the call might have gone through
        activeVersion = null;
        
        CreatePolicyVersionRequest request = 
                new CreatePolicyVersionRequest()
                    .withPolicyArn(policyArn)
                    .withPolicyDocument(newPolicy)
                    .withSetAsDefault(true);
        iamClient.createPolicyVersion(request);
    }
    
    public synchronized String getActiveVersion() {
        if (activeVersion != null) {
            return activeVersion;
        }
        
        String marker = null;
        do {
            ListPolicyVersionsResult result = 
                    iamClient.listPolicyVersions(new ListPolicyVersionsRequest().withPolicyArn(policyArn).withMarker(marker));
            Optional<PolicyVersion> match = result.getVersions().stream()
                .filter(v -> Boolean.TRUE.equals(v.isDefaultVersion()))
                .findFirst();
            
            if (match.isPresent()) {
                activeVersion = match.get().getVersionId();
                return activeVersion;
            }
            
            marker = result.getMarker();
        } while (marker != null);
        
        throw new IllegalArgumentException("No active policy version for " + policyArn);
    }
    
    @Override
    protected String fetchPolicyString() {
        return iamClient.getPolicyVersion(
                        new GetPolicyVersionRequest()
                            .withPolicyArn(policyArn)
                            .withVersionId(getActiveVersion()))
                .getPolicyVersion()
                .getDocument();
    }
    
    public void attach(String user) {
        iamClient.attachUserPolicy(
                new AttachUserPolicyRequest()
                    .withPolicyArn(policyArn)
                    .withUserName(user));
    }
}