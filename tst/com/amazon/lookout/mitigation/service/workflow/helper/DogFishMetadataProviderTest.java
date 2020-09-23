package com.amazon.lookout.mitigation.service.workflow.helper;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Rule;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;

import com.amazon.lookout.mitigation.service.activity.helper.AwsDogfishPrefixesMetadataFetcher;
import com.amazon.lookout.mitigation.service.workflow.helper.DogFishMetadataProvider;
import com.amazon.lookout.models.prefixes.DogfishIPPrefix;
import com.amazon.lookout.models.prefixes.DogfishJSON;
import com.amazon.lookout.ip.trie.AllIpVersionsCidrsTrie;
import com.amazon.lookout.ip.IPUtils;

import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class DogFishMetadataProviderTest {
    
    private AwsDogfishPrefixesMetadataFetcher awsDogfishFetcher;
    private DogfishJSON awsDogfishJSON;
    private DogFishMetadataProvider awsDogFishMetadataProvider;
    private AllIpVersionsCidrsTrie<DogfishIPPrefix> prefixTrie = new AllIpVersionsCidrsTrie<>();
    private final List<DogfishIPPrefix> ipPrefixes = new ArrayList<>();
    private MetricsFactory metricsFactory;
    private static Metrics metrics = Mockito.mock(Metrics.class);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    String[] ipv4Prefixes = { "103.246.150.0/24", "10.12.13.161/32", "54.231.226.0/32", "103.246.150.122/32" };
    String[] ipv4 = { "103.246.150.1", "10.12.13.161", "54.231.226.0", "103.246.150.122" };
    String[] ipv4NotInDogfish = { "1.2.3.4", "5.6.7.8", "9.10.11.12", "14.15.16.17" };
    String[] ipv6Prefixes = { "AAAA:F:F:F::/64", "BBBB:F:F:F::/74", "CCCC:F:F:F::/84", "DDDD:F:F:F::/64" };
    String[] ipv6 = { "AAAA:F:F:F:1::", "BBBB:F:F:F:2::", "CCCC:F:F:F::", "DDDD:F:F:F:4::" };
    String[] ipv6NotInDogfish = { "A:B::", "C:D::", "1:2:3:4:5:6:7:8", "E:F:F:F::" };
    String[] networks = { "PROD", "PROD", "PROD", "EC2" };
    String[] services = { "S3", "S3", "S3", null };
    String[] regions = { "NRT", "PDX", "NRT", "ICN" };
    String[] azs = { "NRT7", "PDX1", "NRT12", null };
    String[] types = { "PublicIP", "RFC1918", "PublicIP", "PublicIP" };
    String[] descriptions = { "S3 VIP from S3VIPER for DC nrt7", "S3 VIP from S3VIPER for DC pdx1",
                                     "S3 VIP from S3VIPER for DC nrt12", "http://tt" };
    String[] invalidIpv4 = { "1", "1111.2.3.4", "1.2.3", "invalid", "x.x.x.x", "1.2.3.x", "1.2.3.4." };
    String[] invalidIpv6 = {"A", "A:F::F::F", "AAAAA:F:F:F:1::"};
    String[] invalidIpv4Cidr = { "1.2.3.4/35", "1.2.3.4//24", "1.2.3/24", "invalid", "x.x.x.x", "1.2.3.x" };
    String[] invalidIpv6Cidr = {"AAAA:F:F:F:://64", "AAAA:F:F:F::/640", "AAAA::F:F:F::/64"};

    @Before
    public void setup() {

        for (int i = 0; i < ipv4Prefixes.length; i++) {
            DogfishIPPrefix prefix = new DogfishIPPrefix();
            prefix.setIpPrefix(ipv4Prefixes[i]);
            prefix.setNetwork(networks[i]);
            prefix.setService(services[i]);
            prefix.setRegion(regions[i]);
            prefix.setAz(azs[i]);
            prefix.setType(types[i]);
            prefix.setDescription(descriptions[i]);
            prefix.setIpVersion(DogfishIPPrefix.IP_VERSION_IPV4);
            ipPrefixes.add(prefix);
        }

        for (int i = 0; i < ipv6Prefixes.length; i++) {
            DogfishIPPrefix prefix = new DogfishIPPrefix();
            prefix.setIpPrefix(ipv6Prefixes[i]);
            prefix.setNetwork(networks[i]);
            prefix.setService(services[i]);
            prefix.setRegion(regions[i]);
            prefix.setAz(azs[i]);
            prefix.setType(types[i]);
            prefix.setDescription(descriptions[i]);
            prefix.setIpVersion(DogfishIPPrefix.IP_VERSION_IPV6);
            ipPrefixes.add(prefix);
        }
    
        for (DogfishIPPrefix prefix : ipPrefixes) {  
            prefixTrie.insert(IPUtils.parseCidr(prefix.getIpPrefix()), prefix); 
        }

        metricsFactory = mock(MetricsFactory.class);
        // mock TSDMetric
        Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
        Mockito.doReturn(metrics).when(metrics).newMetrics();


        awsDogfishFetcher = mock(AwsDogfishPrefixesMetadataFetcher.class);
        awsDogfishJSON = mock(DogfishJSON.class);
        DateTime currentTime = new DateTime();

        Mockito.doNothing().when(awsDogfishFetcher).ensureLoadAttempted();
        when(awsDogfishFetcher.isDataReady()).thenReturn(true);
        when(awsDogfishFetcher.getCurrentObject()).thenReturn(awsDogfishJSON);
        when(awsDogfishFetcher.getLastUpdateTimestamp()).thenReturn(Optional.of(currentTime.minusHours(5)),
                Optional.of(currentTime.minusHours(3)), Optional.of(currentTime.minusHours(3)));
        when(awsDogfishJSON.getPrefixes()).thenReturn(ipPrefixes);

        awsDogFishMetadataProvider = spy(new DogFishMetadataProvider(awsDogfishFetcher, metricsFactory));

        TestUtils.configureLogging();
    }

    @Test
    public void updateCache() {
        String testIP = "32.31.45.11/24";

        awsDogFishMetadataProvider.fetchFile();
        DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(testIP);
        Assert.assertEquals(fetchedPrefix, null);

        int index = 0;
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setIpPrefix(testIP);
        prefix.setNetwork(networks[index]);
        prefix.setService(services[index]);
        prefix.setRegion(regions[index]);
        prefix.setAz(azs[index]);
        prefix.setType(types[index]);
        prefix.setDescription(descriptions[index]);
        prefix.setIpVersion(DogfishIPPrefix.IP_VERSION_IPV4);
        ipPrefixes.add(prefix);

        awsDogFishMetadataProvider.fetchFile();
        fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(testIP);
        Assert.assertEquals(fetchedPrefix.getIpPrefix(), testIP);
    }

    /**
     * Test host status retrieval works
     */
    @Test
    public void testMetadataRetrieval() {
        for (int i = 0; i < ipv4Prefixes.length; i++) {
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv4Prefixes[i]);
            Assert.assertEquals(fetchedPrefix.getIpPrefix(), ipv4Prefixes[i]);
        }

        for (int i = 0; i < ipv6Prefixes.length; i++) { 
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv6Prefixes[i]);
            Assert.assertEquals(fetchedPrefix.getIpPrefix(), ipv6Prefixes[i]);
        }

        for (int i = 0; i < ipv4.length; i++) {
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv4[i]);
            Assert.assertEquals(fetchedPrefix.getIpPrefix(), ipv4Prefixes[i]);
        }

        for (int i = 0; i < ipv6.length; i++) {
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv6[i]);
            Assert.assertEquals(fetchedPrefix.getIpPrefix(), ipv6Prefixes[i]);
        }

        for (int i = 0; i < ipv4NotInDogfish.length; i++) {
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv4NotInDogfish[i]);
            Assert.assertEquals(fetchedPrefix, null);
        }

        for (int i = 0; i < ipv6NotInDogfish.length; i++) {
            DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(ipv6NotInDogfish[i]);
            Assert.assertEquals(fetchedPrefix, null);
        }
    }

    /**
     * Test null
     */
    @Test
    public void testNullDogfish() {
        awsDogfishFetcher = mock(AwsDogfishPrefixesMetadataFetcher.class);
        awsDogfishJSON = mock(DogfishJSON.class);
        DateTime currentTime = new DateTime();

        Mockito.doNothing().when(awsDogfishFetcher).ensureLoadAttempted();
        when(awsDogfishFetcher.isDataReady()).thenReturn(false);
        when(awsDogfishFetcher.getCurrentObject()).thenReturn(null);
        
        awsDogFishMetadataProvider = spy(new DogFishMetadataProvider(awsDogfishFetcher, metricsFactory)); 
        String testIP = "32.31.45.11/24";

        thrown.expect(IllegalArgumentException.class);
        awsDogFishMetadataProvider.attemptToFetchFile();
    }

    /**
     * Test Invalid IPs and CIDRs
     */
    @Test
    public void testInvalidIPAndCidr() {
        String testIP = "32.31.45.11/24";

        awsDogFishMetadataProvider.fetchFile();
        DogfishIPPrefix fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(testIP);
        Assert.assertEquals(fetchedPrefix, null);

        int index = 0;
        DogfishIPPrefix prefix = new DogfishIPPrefix();
        prefix.setIpPrefix(testIP);
        prefix.setNetwork(networks[index]);
        prefix.setService(services[index]);
        prefix.setRegion(regions[index]);
        prefix.setAz(azs[index]);
        prefix.setType(types[index]);
        prefix.setDescription(descriptions[index]);
        prefix.setIpVersion(DogfishIPPrefix.IP_VERSION_IPV4);
        ipPrefixes.add(prefix);

        awsDogFishMetadataProvider.fetchFile();
        fetchedPrefix = awsDogFishMetadataProvider.getCIDRMetaData(testIP);
        Assert.assertEquals(fetchedPrefix.getIpPrefix(), testIP);
    }
}
