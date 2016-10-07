package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Map;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.models.prefixes.DogfishIPPrefix;

import lombok.NonNull;

public class DogFishValidationHelper {
    
    private final String region;
    private final boolean isMasterRegion;
    private final DogFishMetadataProvider dogfishMetadata;
    private final Map<String, String> regionEndpoints;
    private final String masterEndpoint;
    
    public DogFishValidationHelper(@NonNull String region, @NonNull String masterRegion, 
            @NonNull DogFishMetadataProvider dogfishMetadata, @NonNull Map<String, String> regionEndpoints) {
        Validate.notNull(region, "Region cannot be null!");
        Validate.notNull(masterRegion, "Master region cannot be null!");
        this.region = region;
        this.isMasterRegion = masterRegion.equals(region);
        this.dogfishMetadata = dogfishMetadata;
        this.regionEndpoints = regionEndpoints;
        Validate.notNull(regionEndpoints.get(region), "Region is not found in the region endpoint map!");
        masterEndpoint = regionEndpoints.get(masterRegion);
        Validate.notNull(regionEndpoints.get(masterRegion), "Master region is not found in the region endpoint map!");
    }

    public void validateCIDRInRegion(String cidr) {
        DogfishIPPrefix prefix = dogfishMetadata.getCIDRMetaData(cidr);
        if (prefix == null) {
            throw new IllegalArgumentException(String.format("CIDR:%s not found in DogFish lookup!", cidr));
        }
        String fetchedRegion = prefix.getRegion();
        if (fetchedRegion == null) {
            throw new RuntimeException(String.format("CIDR:%s region was null in DogFish lookup!", cidr));
        }
        if (region.equals(fetchedRegion)) {
            return;
        } else if (regionEndpoints.containsKey(fetchedRegion)) {
            String message = String.format("The resource:%s was found in a region with an active mitigation service, "
                    + "please use that regional endpoint to create a mitigation for the resource. Regional endpoint:%s", 
                    cidr, regionEndpoints.get(fetchedRegion));
            throw new IllegalArgumentException(message);
        } else if (isMasterRegion) {
            return;
        }
        String message = String.format("The resource:%s was found in a region without an active mitigation service, "
                + "please use the master region to create a mitigation for the resource. Master endpoint:%s", 
                cidr, masterEndpoint);
        throw new IllegalArgumentException(message);
    }  
}
