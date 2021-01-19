package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DogfishHelperTest {

    @Test
    public void testGetDogfishRole() {
        assertEquals(DogfishHelper.getDogfishRole("us-west-2", "gamma"),
                "arn:aws:iam::806199016981:role/dogfish-ro-global.gamma.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("ap-northeast-1", "gamma"),
                "arn:aws:iam::806199016981:role/dogfish-ro-global.gamma.blackwatch.aws.internal");

        assertEquals(DogfishHelper.getDogfishRole("us-west-1", "prod"),
                "arn:aws:iam::806199016981:role/dogfish-ro-global.prod.blackwatch.aws.internal");

        assertEquals(DogfishHelper.getDogfishRole("us-east-1", "prod-border"),
                "arn:aws:iam::806199016981:role/dogfish-ro-global.prod.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("cn-north-1", "prod-cn"),
                "arn:aws-cn:iam::151819645844:role/dogfish-ro-global.prod.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("cn-northwest-1", "prod-cn"),
                "arn:aws-cn:iam::151819645844:role/dogfish-ro-global.prod.blackwatch.aws.internal");

        assertEquals(DogfishHelper.getDogfishRole("ap-east-1", "prod"),
                "arn:aws:iam::903813587605:role/dogfish-ro-global.prod.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("me-south-1", "prod"),
                "arn:aws:iam::834753366767:role/dogfish-ro-global.prod.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("af-south-1", "prod"),
                "arn:aws:iam::887735306953:role/dogfish-ro-global.prod.blackwatch.aws.internal");
        assertEquals(DogfishHelper.getDogfishRole("eu-south-1", "prod"),
                "arn:aws:iam::017855775129:role/dogfish-ro-global.prod.blackwatch.aws.internal");
    }

    @Test
    public void testGetDogfishS3Bucket() {
        assertEquals(DogfishHelper.getDogfishS3Bucket("us-east-1"), "aws-dogfish-prod-iad");
        assertEquals(DogfishHelper.getDogfishS3Bucket("us-west-1"), "aws-dogfish-prod-sfo");
        assertEquals(DogfishHelper.getDogfishS3Bucket("us-west-2"), "aws-dogfish-prod-pdx");

        assertEquals(DogfishHelper.getDogfishS3Bucket("cn-north-1"), "aws-dogfish-prod-bjs");
        assertEquals(DogfishHelper.getDogfishS3Bucket("cn-northwest-1"), "aws-dogfish-prod-zhy");

        assertEquals(DogfishHelper.getDogfishS3Bucket("ap-east-1"), "aws-dogfish-prod-hkg");
        assertEquals(DogfishHelper.getDogfishS3Bucket("me-south-1"), "aws-dogfish-prod-bah");
        assertEquals(DogfishHelper.getDogfishS3Bucket("af-south-1"), "aws-dogfish-prod-cpt");
        assertEquals(DogfishHelper.getDogfishS3Bucket("eu-south-1"), "aws-dogfish-prod-mxp");
    }
}
