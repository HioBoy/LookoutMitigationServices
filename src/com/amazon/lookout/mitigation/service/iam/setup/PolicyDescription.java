package com.amazon.lookout.mitigation.service.iam.setup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString @EqualsAndHashCode @RequiredArgsConstructor
public class PolicyDescription {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Getter
    private final String policyName;
    
    @Getter
    private final String fileName;
    
    private transient String policyDocument;
    
    public synchronized String getPolicyDocument() {
        if (policyDocument != null ) return policyDocument;
        
        File policyFile = new File("auth-policies", fileName);
        StringBuilder builder = new StringBuilder();
        char buffer[] = new char[512];
        try (Reader reader = new InputStreamReader(new FileInputStream(policyFile), StandardCharsets.UTF_8)) {
            int charsRead;
            while ((charsRead = reader.read(buffer)) != -1 ) {
                builder.append(buffer, 0, charsRead);
            }
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File " + policyFile.getAbsolutePath() + " does not exist", e);
        } catch (IOException e) {
            // This shouldn't happen so don't force everyone to handle the IOException
            throw new RuntimeException(e);
        }
        // Trim any extra newlines at the end of the file
        while (builder.charAt(builder.length() - 1) == '\n') builder.deleteCharAt(builder.length() - 1);
        
        policyDocument = builder.toString();
        return policyDocument;
    }
    
    /**
     * Return true if the policies are the same
     * @param iamClient client to use to retrieve the stored policy
     * @param storedPolicy
     * @return
     */
    public boolean policyEquals(AmazonIdentityManagement iamClient, StoredPolicy storedPolicy) {
        if (storedPolicy == null) return false;
        
        // Don't do string compare as it may have been reformatted - compare the JSON tree instead 
        JsonNode expectedTree;
        try {
            expectedTree = objectMapper.readTree(getPolicyDocument());
        } catch (IOException e) {
            throw new RuntimeException("Error parsing user policy " + fileName + ": " + e.getMessage(), e);
        }
        
        return expectedTree.equals(storedPolicy.getPolicyDocument());
    }
}