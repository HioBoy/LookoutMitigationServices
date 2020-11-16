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
    private final Map<String, Boolean> handleActiveBWAPIMitigations;
    private final String masterEndpoint;
    
    public DogFishValidationHelper(@NonNull String region, @NonNull String masterRegion, 
            @NonNull DogFishMetadataProvider dogfishMetadata, @NonNull Map<String, String> regionEndpoints,
            @NonNull Map<String, Boolean> handleActiveBWAPIMitigations) {
        Validate.notNull(region, "Region cannot be null!");
        Validate.notNull(masterRegion, "Master region cannot be null!");
        this.region = region;
        this.isMasterRegion = masterRegion.equals(region);
        this.dogfishMetadata = dogfishMetadata;
        this.regionEndpoints = regionEndpoints;
        this.handleActiveBWAPIMitigations = handleActiveBWAPIMitigations;
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
        // assuming region (the region mitigation service is running in) will never be equal to "global"
        if (region.equals(fetchedRegion)) {
            return;
        } else if (fetchedRegion.toLowerCase().equals("global") && (region.equals("PDX") || region.equals("IAD") || region.equals("SFO"))) {
            return;
        } else if (fetchedRegion.toLowerCase().equals("global")) {
            String message = String.format("The resource:%s was found to be a global resource, "
                    + "please use the global mitigation service in PDX to create a mitigation for the resource."
                    + " Regional endpoint:%s", 
                    cidr, regionEndpoints.get(fetchedRegion));
            throw new IllegalArgumentException(message);
        } else if (regionEndpoints.containsKey(fetchedRegion) && Boolean.TRUE.equals(handleActiveBWAPIMitigations.get(fetchedRegion))) {
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
