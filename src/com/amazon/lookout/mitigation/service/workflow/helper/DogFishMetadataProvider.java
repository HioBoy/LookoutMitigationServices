package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.service.activity.helper.AwsDogfishPrefixesMetadataFetcher;
import com.amazon.lookout.ip.IPUtils;
import com.amazon.lookout.ip.trie.AllIpVersionsCidrsTrie;
import com.amazon.lookout.models.prefixes.DogfishIPPrefix;
import com.amazon.lookout.models.prefixes.DogfishJSON;
import com.amazon.lookout.ip.IpCidr;

/**
 * This class is responsible for downloading dogFish File and updating the online data structure
 * It is executed periodically and checks if the local data structure needs to be updated
 * It also gives a function to the user to get the metadata associated with an IP or a CIDR
 * For this they will have to call the getCIDRMetaData() function 
 *  
 */
@ThreadSafe
public class DogFishMetadataProvider implements Runnable {
    private static final Log LOG = LogFactory.getLog(DogFishMetadataProvider.class);
    private final AwsDogfishPrefixesMetadataFetcher dogfishFetcher;
    private Optional<DateTime> lastAwsDogfishUpdateTimestamp = Optional.empty();
    private DateTime lastUpdateTimestamp = DateTime.now();
    private final MetricsFactory metricsFactory;
    private static volatile AllIpVersionsCidrsTrie<DogfishIPPrefix> cidrToPrefixMetadataTrie;

    @ConstructorProperties({"dogfishFetcher", "metricsFactory"})
    public DogFishMetadataProvider(@NonNull AwsDogfishPrefixesMetadataFetcher dogfishFetcher, 
                            @NonNull MetricsFactory metricsFactory) {
        this.dogfishFetcher = dogfishFetcher;
        this.metricsFactory = metricsFactory;
    }

    @Override
    public void run() {
        try {
            fetchFile();
        }
        catch (Exception ex) {
            LOG.error("A problem occurred when running the dog fish file fetcher.", ex);
        }
    }
    
    public synchronized void fetchFile() {
        LOG.info("Running Periodic Worker to check if dog fish file download is required");
        try (TSDMetrics metrics = new TSDMetrics(metricsFactory, "RunDogFishMetadataProvider")) {
            final DogfishJSON awsDogfishJSON = dogfishFetcher.getCurrentObject();
            final Optional<DateTime> latestAwsDogfishUpdateTimeStamp = dogfishFetcher.getLastUpdateTimestamp();

            LOG.info("latestAwsDogfishUpdateTimestamp : " + latestAwsDogfishUpdateTimeStamp);
            LOG.info("lastAwsDogfishUpdateTimestamp : " + lastAwsDogfishUpdateTimestamp);
            if (cidrToPrefixMetadataTrie == null ||
                isNewerThanCache(awsDogfishJSON, lastAwsDogfishUpdateTimestamp, latestAwsDogfishUpdateTimeStamp)) {
                LOG.info("Updating the dogfish prefix trie");
                lastAwsDogfishUpdateTimestamp = latestAwsDogfishUpdateTimeStamp;
                lastUpdateTimestamp = DateTime.now();
                buildPrefixTrie(awsDogfishJSON);
            } else {
                LOG.info("The local copy and remote S3 copy of the dog fish file is same. No need to download new dog fish data");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void buildPrefixTrie(DogfishJSON awsDogfishJSON) {
        Validate.notNull(awsDogfishJSON);
        Validate.notEmpty(awsDogfishJSON.getPrefixes());
        dogfishFetcher.ensureLoadAttempted();

        AllIpVersionsCidrsTrie<DogfishIPPrefix> trie = new AllIpVersionsCidrsTrie<>();
        for (DogfishIPPrefix prefix : awsDogfishJSON.getPrefixes()) {
            try {
                trie.insert(IPUtils.parseCidr(prefix.getIpPrefix()), prefix);
            }
            catch (Exception e) {
                LOG.warn("[INVALID_PREFIX] Could not enter metadata to trie for prefix: " + prefix.getIpPrefix());
            }
        }
        cidrToPrefixMetadataTrie = trie;
    }

    // update dogfish trie if cache data is too old (> 1 day old) or downloaded data is newer than existing trie
    private boolean isNewerThanCache(DogfishJSON dogfishJSON, Optional<DateTime> lastLocalDogfishJSONUpdateTimeStamp,
        Optional<DateTime> dogfishJSONTimeStamp) {
        return (null != dogfishJSON
            && (!lastLocalDogfishJSONUpdateTimeStamp.isPresent()
            || this.lastUpdateTimestamp.isBefore(DateTime.now().minusDays(1))
            || lastLocalDogfishJSONUpdateTimeStamp.get().isBefore(dogfishJSONTimeStamp.get())));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})  // due to legacy lookout code, not fixing
    private DogfishIPPrefix getMetadata(final String ipOrCidr) {
        if (cidrToPrefixMetadataTrie == null) {
            String msg = "Problem with downloaded dogfish file";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        IpCidr cidr;
        try {
            cidr = IPUtils.parseCidr(IPUtils.convertToCidr(ipOrCidr));
        } 
        catch (Exception e) {
            String msg = "Failed to retrieve metadata as input is not valid CIDR or IP: " + ipOrCidr;
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }

        DogfishIPPrefix prefix = cidrToPrefixMetadataTrie.longestPrefixMatchingSearch(cidr.getIp(), cidr.getPrefixLength());
   
        return prefix;
    }

    private static void validateCidr(String cidr) {
        
        int slashIndex = cidr.lastIndexOf('/');

        if (slashIndex != -1 && !IPUtils.isValidCIDR(cidr)) {
            String msg = "Invalid CIDR "+ cidr;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        else if (slashIndex == -1 && !IPUtils.isValidIp(cidr)) {
            String msg = "Invalid IP "+ cidr;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    public DogfishIPPrefix getCIDRMetaData(String cidr) {
        Validate.notEmpty(cidr);

        try {
            validateCidr(cidr);
            return getMetadata(cidr);
        }
        catch (Exception e) {
            LOG.error("Problem occured while getting metadata from the dog fish data structure ", e);
            return null;
        }
    }
}
