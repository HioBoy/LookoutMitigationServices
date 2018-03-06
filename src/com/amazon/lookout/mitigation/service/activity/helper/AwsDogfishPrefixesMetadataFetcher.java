package com.amazon.lookout.mitigation.service.activity.helper;

import java.beans.ConstructorProperties;
import java.util.List;

import lombok.NonNull;

import amazon.odin.awsauth.OdinAWSCredentialsProvider;

import com.amazon.aws158.commons.fetcher.FetchFromS3AndSaveAsJsonStringToDisk;
import com.amazon.aws158.commons.fetcher.RefreshedObjectConsumer;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.models.prefixes.DogfishJSON;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Class for fetching metadata for Amazon prefixes from Dogfish.
 * 
 * https://code.amazon.com/packages/LookoutNetworkInformationManager/blobs/mainline/--/src/com/amazon/lookout/prefixes/AwsDogfishPrefixesMetadataFetcher.java
 * 
 */
public class AwsDogfishPrefixesMetadataFetcher extends FetchFromS3AndSaveAsJsonStringToDisk<DogfishJSON> {
    private static final String METHOD_NAME = "AwsDogfishPrefixesMetadataFetcher";
    private static final String AMAZON_PREFIXES_S3_KEY = "amazon-prefixes.json";
    private static final String AMAZON_PREFIXES_S3_BUCKET_PREFIX = "lookout-prefixes-";

    @ConstructorProperties({ "pathToDiskCache", "fileName", "syncAlarmThresholdMinutes", "acceptableAgeOfCachedContentMinutes", "domain",
            "materialSet", "reportees", "metricsFactory" })
    public AwsDogfishPrefixesMetadataFetcher(String pathToDiskCache, String fileName, int syncAlarmThresholdMinutes,
            int acceptableAgeOfCachedContentMinutes, String domain, @NonNull String materialSet,
            List<RefreshedObjectConsumer<DogfishJSON>> reportees, MetricsFactory metricsFactory) {
        this(pathToDiskCache, fileName, syncAlarmThresholdMinutes, acceptableAgeOfCachedContentMinutes, domain,
                (AmazonS3Client) AmazonS3ClientBuilder.standard().withCredentials(new OdinAWSCredentialsProvider(materialSet)).build(),
                reportees, metricsFactory);
    }

    public AwsDogfishPrefixesMetadataFetcher(@NonNull String pathToDiskCache, @NonNull String fileName, int syncAlarmThresholdMinutes,
            int acceptableAgeOfCachedContentMinutes, @NonNull String domain, @NonNull AmazonS3Client amazonS3Client,
            @NonNull List<RefreshedObjectConsumer<DogfishJSON>> reportees, @NonNull MetricsFactory metricsFactory) {
        super(DogfishJSON.class, pathToDiskCache, fileName, syncAlarmThresholdMinutes, acceptableAgeOfCachedContentMinutes,
                AMAZON_PREFIXES_S3_BUCKET_PREFIX + domain, AMAZON_PREFIXES_S3_KEY,
                amazonS3Client, reportees, metricsFactory, METHOD_NAME);
    }
}

