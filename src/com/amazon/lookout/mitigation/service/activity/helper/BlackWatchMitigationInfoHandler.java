package com.amazon.lookout.mitigation.service.activity.helper;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;

public interface BlackWatchMitigationInfoHandler {
    public void deactivateMitigation(String mitigationId, MitigationActionMetadata actionMetadata);
    public void changeOwnerARN(String mitigationId, String newOwnerARN, String expectedOwnerARN, MitigationActionMetadata actionMetadata);
    public List<BlackWatchMitigationDefinition> getBlackWatchMitigations(
            String mitigationId, String resourceId, String resourceType,
            String ownerARN, long maxNumberOfEntriesToReturn,
            TSDMetrics tsdMetrics);
    
    static final int MAX_RAND_BOUND = 10000;
    static final String MIT_ID_DATE_FORMAT_STRING = "yyyyMMddHHmmssSSS";
    //DateTimeFormatter is immutable and thread safe.
    static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(MIT_ID_DATE_FORMAT_STRING, Locale.US)
            .withZone(ZoneId.of("Z"));
    final Random randomGenerator = new Random();
    
    public default String generateMitigationId() {
        int random = randomGenerator.nextInt(MAX_RAND_BOUND);
        return String.format(Locale.US, "%s_%04d", formatter.format(Instant.now()), random);
    }

    public ApplyBlackWatchMitigationResponse applyBlackWatchMitigation(String resourceId, String resourceType,
            Long globalPPS, Long globalBPS, Integer minsToLive, MitigationActionMetadata metadata,
            String mitigationSettingsJSON, String userARN, TSDMetrics tsdMetrics);
    
    public UpdateBlackWatchMitigationResponse updateBlackWatchMitigation(String mitigationId, Long globalPPS,
            Long globalBPS, Integer minsToLive, MitigationActionMetadata metadata, String mitigationSettingsJSON,
            String userARN, TSDMetrics tsdMetrics);
}
