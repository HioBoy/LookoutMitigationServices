package com.amazon.lookout.mitigation.service.iam.setup;

import lombok.Getter;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.DeleteUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetUserPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetUserPolicyResult;
import com.amazonaws.services.identitymanagement.model.PutUserPolicyRequest;

public class EmbeddedPolicy extends StoredPolicy {
    public static EmbeddedPolicy createPolicy(AmazonIdentityManagement iamClient, String userName, PolicyDescription policy) {
        EmbeddedPolicy newPolicy = new EmbeddedPolicy(iamClient, userName, policy.getPolicyName());
        newPolicy.updatePolicy(policy.getPolicyDocument());
        return newPolicy;
    }
    
    @Getter
    private final String userName;
    
    public EmbeddedPolicy(AmazonIdentityManagement iamClient, String userName, String policyName) {
        super(iamClient, policyName, PolicySource.EMBEDDED);
        this.userName = userName;
    }
    
    @Override
    protected String fetchPolicyString() {
        GetUserPolicyResult policyResult = iamClient.getUserPolicy(
                new GetUserPolicyRequest()
                    .withUserName(userName)
                    .withPolicyName(policyName));
        return policyResult.getPolicyDocument();
    }
    
    @Override
    public void updatePolicy(String newPolicy) {
        PutUserPolicyRequest request = new PutUserPolicyRequest();
        request.setPolicyName(getPolicyName());
        request.setUserName(userName);
        request.setPolicyDocument(newPolicy);
        
        iamClient.putUserPolicy(request);            
    }
    
    public void removePolicy() {
        iamClient.deleteUserPolicy(new DeleteUserPolicyRequest()
            .withUserName(userName)
            .withPolicyName(policyName));
    }
}