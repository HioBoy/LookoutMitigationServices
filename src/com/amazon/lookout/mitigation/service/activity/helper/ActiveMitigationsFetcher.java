package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.status.helper.MitigationInstanceStatusHelper;
import com.amazon.lookout.model.RequestType;

/**
 * Class to help fetch mitigations which are currently deemed as active by the mitigation service, based on their value in the ActiveMitigations DDB table.
 * Implements callable to allow scheduling this fetching task in a separate thread.
 */
@AllArgsConstructor
public class ActiveMitigationsFetcher implements Callable<List<MitigationRequestDescriptionWithStatuses>> {
    
    protected static final String KEY_SEPARATOR = "#";
    
    @NonNull private final String serviceName;
    @NonNull private final String deviceName;
    private final List<String> locationsConstraint; // List of locations to constraint the result by, could be null.
    @NonNull private final ActiveMitigationInfoHandler activeMitigationInfoHandler;
    @NonNull private final RequestInfoHandler requestInfoHandler;
    @NonNull private final TSDMetrics tsdMetrics;
    
    /**
     * Queries ActiveMitigations table to get a list of mitigations currently deemed as active by the mitigation table.
     * For each of such mitigation, fetch the mitigation information from the MitigationRequests table. After consolidating all locations for the same mitigation into the 
     * same MitigationRequestDescriptionWithStatuses instance, we return back a list of such MitigationRequestDescriptionWithStatuses instances, describing the mitigation
     * along with the list of locations that this mitigation is successfully deployed to.
     */
    public List<MitigationRequestDescriptionWithStatuses> call() {
        TSDMetrics subMetrics = tsdMetrics.newSubMetrics("ActiveMitigationsFetcher.call");
        try {
            // Fetch the list of active mitigations from the activeMitigationInfoHandler, which queries the ActiveMitigations DDB table.
            List<ActiveMitigationDetails> listOfActiveMitigationDetails = activeMitigationInfoHandler.getActiveMitigationsForService(serviceName, deviceName, locationsConstraint, subMetrics);
            
            // Create a map of deviceName and jobId to MitigationRequestDescription. We want to condense the information within the ACTIVE_MITIGATIONS
            // table based on deviceName and jobId so that we only query the request table for each unique jobId. Additionally we 
            // also aggregate all of the locations for each item in the table with a unique deviceName and jobId combination into a list that 
            // will be set within each MitigationRequestDescription that this key maps to. 
            Map<String, MitigationRequestDescriptionWithStatuses> descriptionsWithLocationsMap = new HashMap<>();
            
            // Iterate through the list of ActiveMitigationDetails and use the deviceName and jobId to get the remaining information needed for our response.
            for (ActiveMitigationDetails activeMitigationDetails : listOfActiveMitigationDetails) {
                String deviceName = activeMitigationDetails.getDeviceName();
                long jobId = activeMitigationDetails.getJobId();
                String location = activeMitigationDetails.getLocation();
                String key = deviceName + KEY_SEPARATOR + jobId;
                long lastDeployDate = activeMitigationDetails.getLastDeployDate();
                
                if (!descriptionsWithLocationsMap.containsKey(key)) {
                    MitigationRequestDescription mitigationDescription = requestInfoHandler.getMitigationRequestDescription(deviceName, jobId, tsdMetrics);
                    
                    MitigationRequestDescriptionWithStatuses descriptionWithStatuses = new MitigationRequestDescriptionWithStatuses();
                    descriptionWithStatuses.setMitigationRequestDescription(mitigationDescription);
                    
                    // Since this location was identified in the ActiveMitigations table, we set its status to indicate that it was successfully created.
                    MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
                    instanceStatus.setLocation(location);
                    instanceStatus.setDeployDate(lastDeployDate);
                    
                    String successStatus = MitigationInstanceStatusHelper.getOperationSuccessfulStatus(RequestType.valueOf(mitigationDescription.getRequestType()));
                    instanceStatus.setMitigationStatus(successStatus);
                    
                    Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
                    instancesStatus.put(location, instanceStatus);
                    descriptionWithStatuses.setInstancesStatusMap(instancesStatus);
                    
                    // Add this MitigationRequestDescriptionWithStatuses instance to the descriptionsWithLocationsMap to identify other locations for the same mitigation.
                    descriptionsWithLocationsMap.put(key, descriptionWithStatuses);
                } else {
                    MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
                    instanceStatus.setLocation(location);
                    
                    MitigationRequestDescription mitigationDescription = descriptionsWithLocationsMap.get(key).getMitigationRequestDescription();
                    String successStatus = MitigationInstanceStatusHelper.getOperationSuccessfulStatus(RequestType.valueOf(mitigationDescription.getRequestType()));
                    instanceStatus.setMitigationStatus(successStatus);
                    
                    descriptionsWithLocationsMap.get(key).getInstancesStatusMap().put(location, instanceStatus);
                }
            }
            return new ArrayList<>(descriptionsWithLocationsMap.values());
        } finally {
            subMetrics.end();
        }
    }
}
