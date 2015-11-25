package com.amazon.lookout.mitigation.service.activity;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.BlackholeSupportedValues;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.google.common.collect.ImmutableList;

public class BlackholeTestUtils {
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1 = "1mb3LKxxQjawDEB-whgoJA==";
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2 = "2DA3LKxxQjawDEB-whgoVA==";
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1 = "3SD3LKxxQjawDEB-whgoMA==";
    public static final String VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID = "2owaYUvjSUG6jPGZFv2XzQ==";
    public static final String WELL_FORMATTED_BUT_INVALID_TRANSIT_PROVIDER_ID = "3owaYUvjSUG6jPGZFv2XzQ==";
    public static final String INVALID_TRANSIT_PROVIDER_ID = "Invalid";
    
    public static BlackholeMitigationHelper mockMitigationHelper() {
        BlackholeMitigationHelper blackholeMitigationHelper = mock(BlackholeMitigationHelper.class);
        
        TransitProvider validSupportedTransitProvider1 = new TransitProvider();
        validSupportedTransitProvider1.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1);
        validSupportedTransitProvider1.setTransitProviderName("Test Provider");
        validSupportedTransitProvider1.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider1.setTransitProviderCommunity("16509:3 16509:100");
        
        TransitProvider validSupportedTransitProvider2 = new TransitProvider();
        validSupportedTransitProvider2.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_2);
        validSupportedTransitProvider2.setTransitProviderName("Test Provider");
        validSupportedTransitProvider2.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider2.setTransitProviderCommunity("16509:3 16509:102");
        
        TransitProvider validSupportedTransitProvider3 = new TransitProvider();
        validSupportedTransitProvider3.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID_12345_1);
        validSupportedTransitProvider3.setTransitProviderName("Test Provider");
        validSupportedTransitProvider3.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider3.setTransitProviderCommunity("12345:3 12345:100");
        
        TransitProvider validUnsupportedTransitProvider = new TransitProvider();
        validUnsupportedTransitProvider.setTransitProviderId(VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID);
        validUnsupportedTransitProvider.setTransitProviderName("Test Provider 2");
        validUnsupportedTransitProvider.setBlackholeSupported(BlackholeSupportedValues.Unsupported);
        
        List<TransitProvider> allTransitProviders = ImmutableList.of(
                        validSupportedTransitProvider1, validSupportedTransitProvider2,
                        validSupportedTransitProvider3, validUnsupportedTransitProvider);
        
        when(blackholeMitigationHelper.listTransitProviders(any(TSDMetrics.class)))
            .thenReturn(allTransitProviders);
        
        Map<String, TransitProvider> transitProviderMap = 
                allTransitProviders.stream().collect(Collectors.toMap(t -> t.getTransitProviderId(), t -> t));
        
        when(blackholeMitigationHelper.loadBlackholeDevice(anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> Optional.ofNullable(transitProviderMap.get(i.getArguments()[0])));
        
        return blackholeMitigationHelper;
    }
}
