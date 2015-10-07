package com.amazon.lookout.mitigation.service.iam.setup;

import java.io.IOException;
import java.net.URLDecoder;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RequiredArgsConstructor
abstract class StoredPolicy {
    private static final class FourSpaceIndenter implements DefaultPrettyPrinter.Indenter {
        public static final FourSpaceIndenter INSTANCE = new FourSpaceIndenter();
        
        @Override
        public boolean isInline() {
            return false;
        }

        @Override
        public void writeIndentation(JsonGenerator jsonGenerator, int level) throws IOException, JsonGenerationException {
            jsonGenerator.writeRaw('\n');
            for( int i = 0; i < level; ++i) {
                jsonGenerator.writeRaw("    ");
            }
        }
    }
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final DefaultPrettyPrinter prettyPrinter = 
            new DefaultPrettyPrinter().withObjectIndenter(FourSpaceIndenter.INSTANCE)
                                      .withArrayIndenter(FourSpaceIndenter.INSTANCE);
    
    public enum PolicySource {
        EMBEDDED,
        ATTACHED
    }
    
    protected final AmazonIdentityManagement iamClient;
    
    @Getter
    protected final String policyName;
    
    @Getter
    protected final PolicySource policySource;
    
    private transient JsonNode policyDocument;
    
    public abstract void updatePolicy(String newPolicy);
    
    public synchronized JsonNode getPolicyDocument() {
        if (policyDocument == null) {
            policyDocument = parsePolicyDocument(policyName, fetchPolicyString());
        }
        return policyDocument;
    }

    protected abstract String fetchPolicyString();
    
    public String getFormattedPolicyDocument() {
        String formattedPolicy;
        try {
            formattedPolicy = objectMapper.writer(prettyPrinter).writeValueAsString(getPolicyDocument());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return formattedPolicy;
    }
    
    private static JsonNode parsePolicyDocument(String policyName, String policyDocument) {
        try {
            // Why do they url encode this?
            String actualPolicyDocument = URLDecoder.decode(policyDocument, "utf8");
            return objectMapper.readTree(actualPolicyDocument);
        } catch (IOException e) {
            throw new RuntimeException("Error parsing stored policy " + policyName + ": " + e.getMessage(), e);
        }
    }
    
    
}