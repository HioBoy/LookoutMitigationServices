package com.amazon.lookout.mitigation.service.activity;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.BlackholeSupportedValues;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.google.common.collect.ImmutableList;

public class BlackholeTestUtils {
    public static final String VALID_SUPPORTED_TRANSIT_PROVIDER_ID = "1mb3LKxxQjawDEB-whgoJA==";
    public static final String VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID = "2owaYUvjSUG6jPGZFv2XzQ==";
    public static final String WELL_FORMATTED_BUT_INVALID_TRANSIT_PROVIDER_ID = "3owaYUvjSUG6jPGZFv2XzQ==";
    public static final String INVALID_TRANSIT_PROVIDER_ID = "Invalid";
    
    public static BlackholeMitigationHelper mockMitigationHelper() {
        BlackholeMitigationHelper blackholeMitigationHelper = mock(BlackholeMitigationHelper.class);
        
        TransitProvider validSupportedTransitProvider = new TransitProvider();
        validSupportedTransitProvider.setTransitProviderId(VALID_SUPPORTED_TRANSIT_PROVIDER_ID);
        validSupportedTransitProvider.setTransitProviderName("Test Provider");
        validSupportedTransitProvider.setBlackholeSupported(BlackholeSupportedValues.Supported);
        validSupportedTransitProvider.setTransitProviderCommunity("16509:3 16509:100");
        
        TransitProvider validUnsupportedTransitProvider = new TransitProvider();
        validUnsupportedTransitProvider.setTransitProviderId(VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID);
        validUnsupportedTransitProvider.setTransitProviderName("Test Provider 2");
        validUnsupportedTransitProvider.setBlackholeSupported(BlackholeSupportedValues.Unsupported);
        
        when(blackholeMitigationHelper.listTransitProviders(any(TSDMetrics.class)))
            .thenReturn(ImmutableList.of(validSupportedTransitProvider, validUnsupportedTransitProvider));
        
        when(blackholeMitigationHelper.loadBlackholeDevice(anyString(), any(TSDMetrics.class)))
            .thenAnswer(i -> {
               switch((String) i.getArguments()[0]) {
               case VALID_SUPPORTED_TRANSIT_PROVIDER_ID:
                   return Optional.of(validSupportedTransitProvider);
               case VALID_UNSUPPORTED_TRANSIT_PROVIDER_ID:
                   return Optional.of(validUnsupportedTransitProvider);
               default:
                   return Optional.empty();
               }
            });
        
        return blackholeMitigationHelper;
    }
}
