package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.google.common.base.Optional;

@AllArgsConstructor
public class ServiceLocationsHelper {
    private static final Log LOG = LogFactory.getLog(ServiceLocationsHelper.class);
    
    @NonNull private final EdgeLocationsHelper edgeLocationsHelper;
    
    public Optional<Set<String>> getLocationsForService(@NonNull String serviceName) {
        switch(serviceName) {
        case ServiceName.Route53:
            return Optional.of(edgeLocationsHelper.getAllClassicPOPs());
        default:
            LOG.warn("ServiceLocationsHelper currently not configured to return locations for serviceName: " + serviceName);
        }
        return Optional.absent();
    }
}
