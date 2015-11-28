package com.amazon.lookout.mitigation.service.activity;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.BlackholeSupportedValues;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class BlackholeTestUtils {
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1 = "1mb3LKxxQjawDEB-whgoJA==";
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_16509_1 = "Test Provider 16509-1";
    
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2 = "2DA3LKxxQjawDEB-whgoVA==";
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_16509_2 = "Test Provider 16509-2";
    
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1 = "3SD3LKxxQjawDEB-whgoMA==";
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_12345_1 = "Test Provider 12345-1";
    
    public static final String VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID = "2owaYUvjSUG6jPGZFv2XzQ==";
    public static final String VALID_UNSUPPORTED_TRANSIT_PROVIDER_NAME = "Test Provider Unsupported";
    
    public static final String WELL_FORMATTED_BUT_INVALID_TRANSIT_PROVIDER_ID = "3owaYUvjSUG6jPGZFv2XzQ==";
    public static final String INVALID_TRANSIT_PROVIDER_ID = "Invalid";
    
    public static final String DEVICE_16509 = "Device16509";
    public static final String DEVICE_12345 = "Device12345";
    
    public static BlackholeMitigationHelper mockMitigationHelper() {
        BlackholeMitigationHelper blackholeMitigationHelper = mock(BlackholeMitigationHelper.class);
        
        TransitProvider validSupportedTransitProvider1 = new TransitProvider();
        validSupportedTransitProvider1.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1);
        validSupportedTransitProvider1.setTransitProviderName(VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_16509_1);
        validSupportedTransitProvider1.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider1.setTransitProviderCommunity("16509:3 16509:100");
        
        TransitProvider validSupportedTransitProvider2 = new TransitProvider();
        validSupportedTransitProvider2.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2);
        validSupportedTransitProvider2.setTransitProviderName(VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_16509_2);
        validSupportedTransitProvider2.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider2.setTransitProviderCommunity("16509:3 16509:102");
        
        TransitProvider validSupportedTransitProvider3 = new TransitProvider();
        validSupportedTransitProvider3.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1);
        validSupportedTransitProvider3.setTransitProviderName(VALID_SUPPORTED_TRANSIT_PROVIDER_NAME_12345_1);
        validSupportedTransitProvider3.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider3.setTransitProviderCommunity("12345:3 12345:100");
        
        TransitProvider validUnsupportedTransitProvider = new TransitProvider();
        validUnsupportedTransitProvider.setTransitProviderId(VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID);
        validUnsupportedTransitProvider.setTransitProviderName(VALID_UNSUPPORTED_TRANSIT_PROVIDER_NAME);
        validUnsupportedTransitProvider.setBlackholeSupported(BlackholeSupportedValues.Unsupported);
        
        List<TransitProvider> allTransitProviders = ImmutableList.of(
                        validSupportedTransitProvider1, validSupportedTransitProvider2,
                        validSupportedTransitProvider3, validUnsupportedTransitProvider);
        
        when(blackholeMitigationHelper.listTransitProviders(any(TSDMetrics.class)))
            .thenReturn(allTransitProviders);
        
        Map<String, TransitProvider> transitProviderMap = 
                allTransitProviders.stream().collect(Collectors.toMap(t -> t.getTransitProviderId(), t -> t));
        
        when(blackholeMitigationHelper.loadTransitProvider(anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> Optional.ofNullable(transitProviderMap.get(i.getArguments()[0])));
        
        BlackholeDevice device16509 = new BlackholeDevice();
        device16509.setDeviceName(DEVICE_16509);
        device16509.setDeviceDescription("Device for 16509 community");
        device16509.setSupportedASNs(ImmutableSet.of(16509));
        
        BlackholeDevice device1234 = new BlackholeDevice();
        device1234.setDeviceName(DEVICE_12345);
        device1234.setDeviceDescription("Device for 12345 community");
        device1234.setSupportedASNs(ImmutableSet.of(12345));
        
        List<BlackholeDevice> allDevices = ImmutableList.of(device16509, device1234);
        
        when(blackholeMitigationHelper.listBlackholeDevices(any(TSDMetrics.class)))
            .thenReturn(allDevices);
            
        Map<String, BlackholeDevice> deviceMap = allDevices.stream().collect(Collectors.toMap(d -> d.getDeviceName(), d -> d));
        
        when(blackholeMitigationHelper.loadBlackholeDevice(anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> Optional.ofNullable(deviceMap.get(i.getArguments()[0])));
        
        when(blackholeMitigationHelper.getDevicesForCommunity(anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> BlackholeMitigationHelper.getDevicesForCommunity(allDevices, (String) i.getArguments()[0]));
        
        when(blackholeMitigationHelper.getCommunityString(anyListOf(String.class), anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> {
                @SuppressWarnings("unchecked")
                List<String> providerIds = (List<String>) i.getArguments()[0];
                String additionalCommunities = (String) i.getArguments()[1];
                
                Stream<String> stream = providerIds.stream()
                        .map(id -> Optional.ofNullable(transitProviderMap.get(id))
                                .<IllegalArgumentException>orElseThrow(() -> new IllegalArgumentException()))
                        .map(p -> p.getTransitProviderCommunity());
                if (additionalCommunities != null) {
                    stream = Stream.concat(stream, Stream.of(additionalCommunities));
                }
                
                return BlackholeMitigationHelper.getCommunityString(stream);
            });
        
        return blackholeMitigationHelper;
    }
}
