package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import amazon.odin.awsauth.OdinAWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.aws.rip.RIPHelper;
import com.aws.rip.models.region.Region;
import com.aws.rip.models.service.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DogfishHelper {
    private static final Log LOG = LogFactory.getLog(DogfishHelper.class);
    private static final String DOGFISH_ROLE_TEMPLATE = "arn:%s:iam::%s:role/dogfish-ro-global.%s.blackwatch.aws.internal";

    private static Service getService(String region, String serviceName) {
        try {
            return getRIPRegion(region).getService(serviceName);
        } catch (Exception ex) {
            String msg = String.format("Caught exception when querying service: %s in region: %s", serviceName, region);
            throw new IllegalArgumentException(msg, ex);
        }
    }

    private static Region getRIPRegion(String region) {
        try {
            return RIPHelper.getRegion(region);
        } catch (Exception ex) {
            String msg = String.format("Caught exception when querying region: %s in RIP", region);
            throw new IllegalArgumentException(msg, ex);
        }
    }

    public static String getDogfishRole(String region, String domain) {
        String dogfishAccount = getService(region, "dogfish").getCustomProperties().get("s3_bucket_account");
        //dogfish role naming: arn:{partition}:iam::{account}:role/dogfish-ro-{your-service-principal}
        //and our service principal is:
        // global.prod.blackwatch.aws.internal for prod
        // global.gamma.blackwatch.aws.internal for gamma
        //currently we use prod-cn as the domain in China and prod-border as the domain in IAD.
        //need convert the domain to prod so we can generate the correct dogfish role to assume
        String dogfishDomain = domain.split("-", 2)[0];;
        return String.format(DOGFISH_ROLE_TEMPLATE, getRIPRegion(region).getArnPartition(),
                dogfishAccount, dogfishDomain);
    }

    public static String getDogfishS3Bucket(String region) {
        return getService(region, "dogfish").getCustomProperties().get("s3_bucket_name");
    }

    public static AmazonS3Client getDogfishS3Client(String region, String domain, OdinAWSCredentialsProvider credential) {
        String stsServiceEndpoint = getService(region, "sts").getEndpoint();
        String dogfishRole = getDogfishRole(region, domain);
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(credential)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(stsServiceEndpoint, region))
                .build();
        String roleSessionName = String.format("BWAPI-Dogfish-%s-%s", region, domain);
        STSAssumeRoleSessionCredentialsProvider stsCredentialProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(dogfishRole, roleSessionName)
                .withStsClient(stsClient)
                .build();

        AmazonS3Client s3Client = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(stsCredentialProvider)
                .build();
        return s3Client;
    }

}
