package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.TransitProviderInfo;

public class TransitProviderConverter {
    
    public static TransitProvider convertTransitProviderInfoRequest(TransitProviderInfo transitProviderInfo) {
        // TODO validate transit provider info here, throw IllegalArgumentException for invalid one
        TransitProvider transitProvider = new TransitProvider();
        transitProvider.setTransitProviderId(transitProviderInfo.getId());
        transitProvider.setTransitProviderName(transitProviderInfo.getProviderName());
        transitProvider.setTransitProviderDescription(transitProviderInfo.getProviderDescription());
        transitProvider.setTransitProviderCommunity(transitProviderInfo.getCommunityString());
        transitProvider.setBlackholeSupported(transitProviderInfo.getBlackholeSupported());
        transitProvider.setManualBlackholeLink(transitProviderInfo.getManualBlackholeLink());
        transitProvider.setVersion(transitProviderInfo.getVersion());
        
        return transitProvider;
    }
    
    public static TransitProviderInfo convertTransitProviderInfoResponse(TransitProvider transitProvider) {

        TransitProviderInfo transitProviderInfo = new TransitProviderInfo();
        transitProviderInfo.setId(transitProvider.getTransitProviderId());
        transitProviderInfo.setProviderName(transitProvider.getTransitProviderName());
        transitProviderInfo.setProviderDescription(transitProvider.getTransitProviderDescription());
        transitProviderInfo.setCommunityString(transitProvider.getTransitProviderCommunity());
        transitProviderInfo.setBlackholeSupported(transitProvider.getBlackholeSupported());
        transitProviderInfo.setManualBlackholeLink(transitProvider.getManualBlackholeLink());
        transitProviderInfo.setVersion(transitProvider.getVersion());
        
        return transitProviderInfo;
    }
}
