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
            List<MitigationRequestDescriptionWithLocations> requestDescriptionsWithLocations = 
                    requestInfoHandler.getOngoingRequestsDescription(serviceName, deviceName, subMetrics);
            for (MitigationRequestDescriptionWithLocations descriptionWithLocation : requestDescriptionsWithLocations) {
                List<String> locations = new ArrayList<>(descriptionWithLocation.getLocations());
                if (!CollectionUtils.isEmpty(locationsConstraint)) {
                    // filter the locations with locationsConstraint, if it is not empty
                    locations.retainAll(locationsConstraint);
                    if (locations.isEmpty()) {
                        // This ongoing request's locations does not include any locations in the filter, so skip this ongoing request
                        continue;
                    }
                }
                 
                // Fetch status for each instance being worked on as part of this ongoing request.
                MitigationRequestDescription description = descriptionWithLocation.getMitigationRequestDescription();
                List<MitigationInstanceStatus> instancesStatus = mitigationInstanceInfoHandler
                        .getMitigationInstanceStatus(description.getDeviceName(), description.getJobId(), subMetrics);
                // locationToStatus stores the status of location that has been created in DDB. 
                // some of the instances might have not been created in DDB, so we will need to set it as RUNNING by default
                Map<String, MitigationInstanceStatus> locationToStatus = new HashMap<>();
                for (MitigationInstanceStatus status : instancesStatus) {
                    locationToStatus.put(status.getLocation(), status);
                }
                
                MitigationRequestDescriptionWithStatuses descriptionWithStatuses = new MitigationRequestDescriptionWithStatuses();
                descriptionWithStatuses.setMitigationRequestDescription(description);
                
                Map<String, MitigationInstanceStatus> instancesStatusMap = new HashMap<>();
                for (String location : locations) {
                    MitigationInstanceStatus status = locationToStatus.get(location);
                    // If we don't have the MitigationStatus for some locations, then simply set the missing ones to be RUNNING.
                    if (status == null) {
                        status = new MitigationInstanceStatus();
                        status.setLocation(location);
                        status.setMitigationStatus(MitigationStatus.RUNNING);
                    }
                    instancesStatusMap.put(location, status);
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
