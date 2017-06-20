package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mock;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazonaws.services.s3.AmazonS3;

public class BlackWatchMitigationTemplateValidatorTest {
    
    @Mock
    protected AmazonS3 s3Client;
    
    private class BlackWatchMitigationTemplateTestValidator extends BlackWatchMitigationTemplateValidator {

        public BlackWatchMitigationTemplateTestValidator() {
            super(s3Client);
        }
        
        @Override
        public String getServiceNameToValidate() {
            throw new UnsupportedOperationException("getServiceNameToValidate");
        }
        
        @Override
        public void validateRequestForTemplateAndDevice(
                MitigationModificationRequest request, String mitigationTemplate,
                DeviceName deviceName, TSDMetrics metrics) {
            throw new UnsupportedOperationException("validateRequestForTemplateAndDevice");
        }
    }

    /**
     * S3Object is valid with bucket, key and md5.
     */
    @Test
    public void testValidateS3ObjectValidWithBucket() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }
    
    /**
     * S3Object also valid with only key and md5 (no bucket).
     */
    @Test
    public void testValidateS3ObjectValidWithoutBucket() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withKey("test_key")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }

    /**
     * S3Object invalid with empty bucket name.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidWithEmptyBucketName() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("")
                .withKey("test_key")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }

    /**
     * S3Object invalid with missing s3 key.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidWithoutKey() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }

    /**
     * S3Object invalid with missing md5 checksum.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidWithoutMd5() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .build(), "Test");
    }
    
    /**
     * S3Object valid with bucket, key and sha256.
     */
    @Test
    public void testValidateS3ObjectValidWithBucket_SHA256() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withSha256("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }
    
    /**
     * S3Object valid with bucket, key and both md5 and sha256.
     */
    @Test
    public void testValidateS3ObjectValidWithBucket_SHA256_MD5() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withSha256("8d777f385d3dfec8815d20f7496026dc")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }
    
    
    /**
     * S3Object invalid with refresh enabled and sha256 set.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectValidWithRefreshedBucket_SHA256() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withSha256("8d777f385d3dfec8815d20f7496026dc")
                .withEnableRefresh(true)
                .build(), "Test");
    }
    
    /**
     * S3Object invalid with refresh enabled and both md5 and sha256 set.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectValidWithRefreshedBucket_SHA256_MD5() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withSha256("8d777f385d3dfec8815d20f7496026dc")
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .withEnableRefresh(true)                
                .build(), "Test");
    }
    
    /**
     * S3Object valid with bucket, key and enable_refresh=true.
     */
    @Test
    public void testValidateS3ObjectValidRefreshedWithBucket() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .build(), "Test");
    }
    
    /**
     * S3Object valid with key and enableRefresh=true (no bucket).
     */
    @Test
    public void testValidateS3ObjectValidRefreshedWithoutBucket() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withKey("test_key")
                .withEnableRefresh(true)
                .build(), "Test");
    }
    
    /**
     * S3Object valid key bucket, key, enableRefresh=true and refreshInterval.
     */
    @Test
    public void testValidateS3ObjectValidRefreshedWithRefreshInterval() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .withRefreshInterval(10)
                .build(), "Test");
    }
    
    /**
     * S3Object invalid with refreshInterval == 0.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidRefreshedWithRefreshIntervalZero() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .withRefreshInterval(0)
                .build(), "Test");
    }
    
    /**
     * S3Object invalid with refreshInterval < 0.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidRefreshedWithNegativeRefreshInterval() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .withRefreshInterval(-10)
                .build(), "Test");
    }
    
    /**
     * S3Object invalid with refreshEnabled=true and md5 set.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidRefreshedWithMd5() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .build(), "Test");
    }
    
    /**
     * S3Object invalid with refreshEnabled=false and refreshInterval set.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateS3ObjectInvalidNotRefreshedWithRefreshInterval() {
        BlackWatchMitigationTemplateValidator.validateS3Object(
                S3Object.builder()
                .withBucket("test_bucket")
                .withKey("test_key")
                .withEnableRefresh(true)
                .withMd5("8d777f385d3dfec8815d20f7496026dc")
                .withRefreshInterval(10)
                .build(), "Test");
    }
    
    /**
     * BlackWatchConfigBasedConstraint valid with valid S3Object for config.
     */
    @Test
    public void testValidateBlackWatchConfigBasedConstraintValidWithConfig() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfig(S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build())
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint valid with valid S3Objects for config and configData.
     */
    @Test
    public void testValidateBlackWatchConfigBasedConstraintValidWithConfigAndData() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfig(S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build())
                .withConfigData(Arrays.asList(
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_1")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build(),
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_2")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build()))
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint invalid missing both config and configData.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateBlackWatchConfigBasedConstraintInvalidMissingConfigAndData() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint invalid with configData but missing config.
     * BlackWatchAgentPython will not currently accept mitigations with configData but no config,
     * so Mitigation Service should also refuse to accept such mitigations.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateBlackWatchConfigBasedConstraintInvalidHasDataMissingConfig() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfigData(Arrays.asList(
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_1")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build(),
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_2")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build()))
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint invalid with null pointer in configData.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateBlackWatchConfigBasedConstraintInvalidNullDataItem() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfigData(Arrays.asList(
                    (S3Object)null,
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_2")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build()))
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint invalid with invalid S3Object as config.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateBlackWatchConfigBasedConstraintInvalidWithInvalidConfig() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfig(S3Object.builder()
                    .withBucket("test_bucket")
                    // missing s3 key
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build())
                .build());
    }
    
    /**
     * BlackWatchConfigBasedConstraint invalid with invalid S3Object in configData.
     */
    @Test(expected=IllegalArgumentException.class)
    public void testValidateBlackWatchConfigBasedConstraintInvalidWithInvalidData() {
        new BlackWatchMitigationTemplateTestValidator().validateBlackWatchConfigBasedConstraint(
                BlackWatchConfigBasedConstraint.builder()
                .withConfig(S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build())
                .withConfigData(Arrays.asList(
                    S3Object.builder()
                    .withBucket("test_bucket")
                    .withKey("test_key_1")
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build(),
                    S3Object.builder()
                    .withBucket("test_bucket")
                    // missing s3 key
                    .withMd5("8d777f385d3dfec8815d20f7496026dc")
                    .build()))
                .build());
    }

}
