package com.amazon.lookout.mitigation.service.activity.helper;

import java.beans.ConstructorProperties;
import java.util.List;

import lombok.NonNull;

import com.amazon.aws158.commons.fetcher.FetchFromS3AndSaveAsJsonStringToDisk;
import com.amazon.aws158.commons.fetcher.RefreshedObjectConsumer;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.models.prefixes.DogfishJSON;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Class for fetching metadata for Amazon prefixes from Dogfish.
 */
public class AwsDogfishPrefixesMetadataFetcher extends FetchFromS3AndSaveAsJsonStringToDisk<DogfishJSON> {
    private static final String METHOD_NAME = "AwsDogfishPrefixesMetadataFetcher";

    @ConstructorProperties({ "pathToDiskCache", "fileName", "syncAlarmThresholdMinutes", "acceptableAgeOfCachedContentMinutes",
            "dogfishS3Bucket", "dogfishS3Key", "dogfishS3Client", "reportees", "metricsFactory" })
    public AwsDogfishPrefixesMetadataFetcher(@NonNull String pathToDiskCache, @NonNull String fileName, int syncAlarmThresholdMinutes,
                                             int acceptableAgeOfCachedContentMinutes, @NonNull String dogfishS3Bucket,
                                             @NonNull String dogfishS3Key, @NonNull AmazonS3Client dogfishS3Client,
                                             @NonNull List<RefreshedObjectConsumer<DogfishJSON>> reportees, @NonNull MetricsFactory metricsFactory) {
        super(DogfishJSON.class, pathToDiskCache, fileName, syncAlarmThresholdMinutes, acceptableAgeOfCachedContentMinutes,
                dogfishS3Bucket, dogfishS3Key, dogfishS3Client, reportees, metricsFactory, METHOD_NAME);
    }
}

