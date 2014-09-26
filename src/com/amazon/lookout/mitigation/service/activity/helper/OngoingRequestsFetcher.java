package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.springframework.util.CollectionUtils;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;

/**
 * A Callable which wraps the task of querying the ongoing workflow requests and then gathering the status of each instance involved in the request.
 *
 */
@AllArgsConstructor
public class OngoingRequestsFetcher implements Callable<List<MitigationRequestDescriptionWithStatuses>> {
    
    @NonNull private final RequestInfoHandler requestInfoHandler;
    @NonNull private final String serviceName;
    @NonNull private final String deviceName;
    private final List<String> locationsConstraint; // List of locations to constraint the results by, can be null.
    @NonNull private final MitigationInstanceInfoHandler mitigationInstanceInfoHandler;
    @NonNull private final TSDMetrics tsdMetrics;
    
    public List<MitigationRequestDescriptionWithStatuses> call() {
        TSDMetrics subMetrics = tsdMetrics.newSubMetrics("OngoingRequestsFetcher.call");
        try {
            // Holds the List of MitigationRequestDescriptionWithStatuses instances to be returned, each of which represents an ongoing
            // request along with the status of each instance (location) being worked on as part of this request.
            List<MitigationRequestDescriptionWithStatuses> descriptionsWithStatuses = new ArrayList<>();
            
            // Fetch the list of ongoing requests along with the locations involved in each of the request.
            List<MitigationRequestDescriptionWithLocations> requestDescriptionsWithLocations = requestInfoHandler.getOngoingRequestsDescription(serviceName, deviceName, subMetrics);
            for (MitigationRequestDescriptionWithLocations descriptionWithLocation : requestDescriptionsWithLocations) {
                MitigationRequestDescription description = descriptionWithLocation.getMitigationRequestDescription();
                
                // Fetch status for each instance being worked on as part of this ongoing request.
                List<MitigationInstanceStatus> instancesStatus = mitigationInstanceInfoHandler.getMitigationInstanceStatus(description.getDeviceName(), description.getJobId(), subMetrics);
                
                // locationsWithStatus references all locations for whom we know their MitigationStatus. 
                List<String> locationsWithStatus = new ArrayList<>();
                for (MitigationInstanceStatus status : instancesStatus) {
                    locationsWithStatus.add(status.getLocation());
                }
                
                // allLocations references all locations which we should consider in the result. 
                // This list if all locations where this request is supposed to operate, if no locations constraint was specified, else we simply use the locationsConstraint.
                List<String> allLocations = new ArrayList<>();
                if (CollectionUtils.isEmpty(locationsConstraint)) {
                    allLocations.addAll(descriptionWithLocation.getLocations());
                } else {
               	    allLocations.addAll(locationsConstraint);
                }
                
                // If we don't have the MitigationStatus for all locations, then set the missing ones to simply have the status as RUNNING.
                if (locationsWithStatus.size() != allLocations.size()) {
                    allLocations.removeAll(locationsWithStatus);
                    for (String locationWithoutStatus : allLocations) {
                        MitigationInstanceStatus status = new MitigationInstanceStatus();
                        status.setLocation(locationWithoutStatus);
                        status.setMitigationStatus(MitigationStatus.RUNNING);
                        
                        instancesStatus.add(status);
                    }
                }
                
                MitigationRequestDescriptionWithStatuses descriptionWithStatuses = new MitigationRequestDescriptionWithStatuses();
                descriptionWithStatuses.setMitigationRequestDescription(description);
                
                Map<String, MitigationInstanceStatus> instancesStatusMap = new HashMap<>();
                for (MitigationInstanceStatus status : instancesStatus) {
                    instancesStatusMap.put(status.getLocation(), status);
                }
                descriptionWithStatuses.setInstancesStatusMap(instancesStatusMap);
                
                // Add this MitigationRequestDescriptionWithStatuses instance to the descriptionsWithStatuses which will be returned back to the caller.
                descriptionsWithStatuses.add(descriptionWithStatuses);
            }
            
            return descriptionsWithStatuses;
        } finally {
            subMetrics.end();
        }
    }
}
