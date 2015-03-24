package com.amazon.lookout.mitigation.service.activity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.jms.IllegalStateException;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;
import org.springframework.util.CollectionUtils;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceResponse;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationsFetcher;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.DDBBasedRouterMetadataHelper;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.OngoingRequestsFetcher;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.status.helper.MitigationInstanceStatusHelper;
import com.amazon.lookout.model.RequestType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ListActiveMitigationsForServiceActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListActiveMitigationsForServiceActivity.class);
    
    private enum ListActiveMitigationsExceptions {
        BadRequest,
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(ListActiveMitigationsExceptions.BadRequest.name(),
                                                                                                      ListActiveMitigationsExceptions.InternalError.name()));
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final ActiveMitigationInfoHandler activeMitigationInfoHandler;
    @NonNull private final RequestInfoHandler requestInfoHandler;
    @NonNull private final MitigationInstanceInfoHandler mitigationInstanceHandler;
    @NonNull private final DDBBasedRouterMetadataHelper routerMetadataHelper;
    @NonNull private final ExecutorService threadPool;
    
    public static final String KEY_SEPARATOR = "#";
    
    @Validated
    @Operation("ListActiveMitigationsForService")
    @Documentation("ListActiveMitigationsForService")
    public @Nonnull ListActiveMitigationsForServiceResponse enact(@Nonnull ListActiveMitigationsForServiceRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ListActiveMitigationsForService.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        try {            
            LOG.info(String.format("ListActiveMitigationsForServiceActivity called with RequestId: %s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            CommonActivityMetricsHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Step 1. Validate your request.
            requestValidator.validateListActiveMitigationsForServiceRequest(request);
            
            String serviceName = request.getServiceName();
            String deviceName = request.getDeviceName();
            List<String> locations = request.getLocations();
            
            // Step 2. Spawn a task to get a list of MitigationRequestDescriptionWithStatuses for mitigations created by the MitigationService which are currently deemed active.
            ActiveMitigationsFetcher activeMitigationsFetcher = new ActiveMitigationsFetcher(serviceName, deviceName, locations, activeMitigationInfoHandler, requestInfoHandler, tsdMetrics);
            Future<List<MitigationRequestDescriptionWithStatuses>> activeMitigationsFuture = threadPool.submit(activeMitigationsFetcher);
            
            // Step 3. Spawn tasks to fetch the mitigations for ongoing requests and for mitigations recorded by router mitigation UI (if this request is for the POP_ROUTER deviceName)
            OngoingRequestsFetcher ongoingRequestsFetcher = new OngoingRequestsFetcher(requestInfoHandler, serviceName, deviceName, locations, mitigationInstanceHandler, tsdMetrics);
            Future<List<MitigationRequestDescriptionWithStatuses>> ongoingRequestsFuture = threadPool.submit(ongoingRequestsFetcher);
            
            Future<List<MitigationRequestDescriptionWithStatuses>> routerMetadataFuture = null;
            if (deviceName.equals(DeviceName.POP_ROUTER.name())) {
                routerMetadataFuture = threadPool.submit(routerMetadataHelper);
            }
            
            // Step 4. Wait for the active mitigations task to be completed.
            List<MitigationRequestDescriptionWithStatuses> descriptionsWithStatusesMap = activeMitigationsFuture.get();
            
            // Step 5. Wait for the ongoing requests to be returned back.
            List<MitigationRequestDescriptionWithStatuses> ongoingRequestsDescriptions = ongoingRequestsFuture.get();
            
            // Step 6. Merge ongoing requests with the currently active ones to get back a Map of String (key formed using deviceName and mitigationName) to a list of MitigationRequestDescriptionWithStatuses.
            Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigations = mergeOngoingRequests(ongoingRequestsDescriptions, descriptionsWithStatusesMap);
            
            // Step 7. Wait for descriptions from router metadata (if we have issued a task for it).
            if (routerMetadataFuture != null) {
                List<MitigationRequestDescriptionWithStatuses> routerMitigationDescriptions = new ArrayList<>();
                // RouterMetadataHelper gives back all router mitigations. If locations constraint is specified, then only include mitigations for that location, 
                // else simply use all the mitigations returned back by the helper.
                if (CollectionUtils.isEmpty(locations)) {
                    routerMitigationDescriptions = routerMetadataFuture.get();
                } else {
                    List<MitigationRequestDescriptionWithStatuses> mitigationsReturnedByHelper = routerMetadataFuture.get();
                    for (MitigationRequestDescriptionWithStatuses routerMitigation : mitigationsReturnedByHelper) {
                        // Each mitigation returned by the router mitigation helper belongs to only 1 location.
                        if (routerMitigation.getInstancesStatusMap().keySet().size() > 1) {
                            String msg = "RouterMetadataHelper is expected to return a single instance of MitigationRequestDescriptionWithStatuses per location, " +
                                         "instead found: " + routerMitigation.getInstancesStatusMap().keySet().size() + " for mitigation: " + routerMitigation +
                                         " in the List API for service: " + serviceName + ", device: " + deviceName + " and locations: " + locations;
                            LOG.error(msg);
                            throw new IllegalStateException(msg);
                        }
                        String location = routerMitigation.getInstancesStatusMap().keySet().iterator().next();
                        if (locations.contains(location)) {
                            routerMitigationDescriptions.add(routerMitigation);
                        }
                    }
                }
                routerMetadataHelper.mergeMitigations(mergedMitigations, routerMitigationDescriptions, serviceName);
            }
            
            ListActiveMitigationsForServiceResponse response = new ListActiveMitigationsForServiceResponse();
            response.setServiceName(request.getServiceName());
            
            List<MitigationRequestDescriptionWithStatuses> requestDescriptionsToReturn = new ArrayList<>();
            for (Map.Entry<String, List<MitigationRequestDescriptionWithStatuses>> entry : mergedMitigations.entrySet()) {
                requestDescriptionsToReturn.addAll(entry.getValue());
            }
            response.setMitigationRequestDescriptionsWithStatuses(requestDescriptionsToReturn);
            LOG.info(String.format("ListMitigationsActivity called with RequestId: %s returned: %s.", requestId, ReflectionToStringBuilder.toString(response)));  
            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in ListActiveMitigationsForServiceActivity for requestId: " + requestId + ", reason: " + ex.getMessage());
            LOG.warn(msg + " for request " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListActiveMitigationsExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for ListActiveMitigationsForServiceActivity for requestId: " + requestId);
            LOG.error(msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListActiveMitigationsExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
    
    /**
     * Helps merge ongoing requests to the currently active requests.
     * @param ongoingRequestsWithStatuses List of MitigationRequestDescriptionWithLocations instances, each instance defining an ongoing request along with the locations where this request is running.
     * @param activeMitigations List of MitigationRequestDescriptionWithLocations instances, each instance representing a request which is considered active by the mitigation service.
     * @return Map of String (key formed using deviceName + KEY_SEPARATOR + mitigationName) to a List of MitigationRequestDescriptionWithStatuses which share the same key. 
     *         Note, in most cases, the list will have just 1 entry. However, to handle an edge case when editing we return a list of values. The edge case is described below:
     *         ActiveMitigations returns a mitigation, which is being edited as part of an ongoing request. In such cases, we want to send back both mitigation descriptions, for the
     *         user to know the current mitigation definition and the one that it is being edited to. For such cases, we would have 2 entries in the value, since both of these mitigation descriptions
     *         are for the same deviceName+mitigationName.
     */
    protected Map<String, List<MitigationRequestDescriptionWithStatuses>> mergeOngoingRequests(@NonNull List<MitigationRequestDescriptionWithStatuses> ongoingRequestsWithStatuses, 
                                                                                               @NonNull List<MitigationRequestDescriptionWithStatuses> activeMitigations) {
        Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigations = new HashMap<>();
        for (MitigationRequestDescriptionWithStatuses activeMitigation : activeMitigations) {
            String key = createDeviceAndMitigationNameKey(activeMitigation);
            if (!mergedMitigations.containsKey(key)) {
                mergedMitigations.put(key, new ArrayList<MitigationRequestDescriptionWithStatuses>());
            }
            mergedMitigations.get(key).add(activeMitigation);
        }
        
        for (MitigationRequestDescriptionWithStatuses ongoingRequest : ongoingRequestsWithStatuses) {
            MitigationRequestDescription ongoingRequestDescription = ongoingRequest.getMitigationRequestDescription();
            RequestType requestType = RequestType.valueOf(ongoingRequestDescription.getRequestType());
            switch (requestType) {
            case CreateRequest:
                mergeOngoingCreateRequest(mergedMitigations, ongoingRequest);
                break;
            case DeleteRequest:
                mergeOngoingDeleteRequest(mergedMitigations, ongoingRequest);
                break;
            case EditRequest:
                mergeOngoingEditRequest(mergedMitigations, ongoingRequest);
                break;
            default:
                String msg = "Currently the List API doesn't handle merging requests of type: " + ongoingRequestDescription.getRequestType() +
                             ". OngoingRequest: " + ongoingRequest;
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
        }
        return mergedMitigations;
    }

    /**
     * Helps merging a ongoing create request with mitigations already identified as active.
     * The algorithm for merging ongoing create requests is straightforward:
     * 1. If this mitigation already exists in the mitigations already identified as active - then make sure all locations from the ongoing request are covered in the active one, adding them if missing.
     * 2. If this mitigation doesn't exist in the mitigations already identified as active - then this request has been issued after the active mitigations were fetched. 
     *    So simply add this ongoing request to the response, but including only the instances that haven't failed the ongoing create request.
     * @param mergedMitigations Map of String (key formed using deviceName and mitigationName) to a list of MitigationRequestDescriptionWithStatuses instances, identifying mitigations which must be returned back to the caller.
     *                          Note, the List exists in the value only to support the case where for the same key (deviceName and mitigationName), we might have an entry for an active mitigation
     *                          and also an ongoing edit request for that same mitigation. 
     * @param ongoingCreateRequest An instance of MitigationRequestDescriptionWithStatuses indicating an ongoing create request.
     */
    private void mergeOngoingCreateRequest(Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigations, MitigationRequestDescriptionWithStatuses ongoingCreateRequest) {
        String ongoingCreateRequestKey = createDeviceAndMitigationNameKey(ongoingCreateRequest);
        
        if (mergedMitigations.containsKey(ongoingCreateRequestKey)) {
            // If we have this same create request in the list of active mitigations, then check if all locations are covered.
            for (MitigationRequestDescriptionWithStatuses activeMitigationDescWithStatus : mergedMitigations.get(ongoingCreateRequestKey)) {
                if (activeMitigationDescWithStatus.getInstancesStatusMap().size() < ongoingCreateRequest.getInstancesStatusMap().size()) {
                    // One or more locations in the ongoing create request are missing from the mergedMitigations, add them here now.
                    for (Map.Entry<String, MitigationInstanceStatus> instanceStatus : ongoingCreateRequest.getInstancesStatusMap().entrySet()) {
                        if (!activeMitigationDescWithStatus.getInstancesStatusMap().containsKey(instanceStatus.getKey())) {
                            activeMitigationDescWithStatus.getInstancesStatusMap().put(instanceStatus.getKey(), instanceStatus.getValue());
                        }
                    }
                }
            }
        } else {
            MitigationRequestDescriptionWithStatuses ongoingCreateRequestForNonFailedInstances = new MitigationRequestDescriptionWithStatuses();
            ongoingCreateRequestForNonFailedInstances.setMitigationRequestDescription(ongoingCreateRequest.getMitigationRequestDescription());
            ongoingCreateRequestForNonFailedInstances.setInstancesStatusMap(new HashMap<String, MitigationInstanceStatus>());
            
            String failedCreateStatus = MitigationInstanceStatusHelper.getOperationFailedStatus(RequestType.CreateRequest);
            for (Map.Entry<String, MitigationInstanceStatus> status : ongoingCreateRequest.getInstancesStatusMap().entrySet()) {
                if (!status.getValue().getMitigationStatus().equals(failedCreateStatus)) {
                    ongoingCreateRequestForNonFailedInstances.getInstancesStatusMap().put(status.getKey(), status.getValue());
                }
            }
            
            // If the ongoing create request has at least 1 non-failed location, include it in the response.
            if (!ongoingCreateRequestForNonFailedInstances.getInstancesStatusMap().isEmpty()) {
                mergedMitigations.put(ongoingCreateRequestKey, Lists.newArrayList(ongoingCreateRequestForNonFailedInstances));
            }
        }
    }
    
    /**
     * Helps merging an ongoing delete request with mitigations already identified as active.
     * The algorithm for merging delete requests is as follows:
     * 1. Check if this mitigation being deleted shows up in the list of merged mitigations (representing active mitigations). If it doesn't, then we missed fetching this mitigation
     *    when querying for the active mitigations. It is possible, that it might be a timing issue, hence simply log a warn for it and just add this ongoing delete request 
     *    to the mergedMitigations and return.
     * 2. Iterate over each instance's delete operation status:
     *    2.1 If an instance has successfully completed the delete operation or is in-progress, then update the corresponding mitigation entry in the mergedMitigations to be the 
     *        status of this delete operation.
     *    2.2 If the delete operation was unsuccessful for an instance, then do nothing for this instance since the currently active mitigation instance hasn't changed any state.
     * @param mergedMitigations Map of String (key formed using deviceName and mitigationName) to a list of MitigationRequestDescriptionWithStatuses instances, identifying mitigations which must be returned back to the caller.
     *                          Note, the List exists in the value only to support the case where for the same key (deviceName and mitigationName), we might have an entry for an active mitigation
     *                          and also an ongoing edit request for that same mitigation.
     * @param ongoingDeleteRequest An instance of MitigationRequestDescriptionWithStatuses indicating an ongoing delete request.
     */
    private void mergeOngoingDeleteRequest(Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigations, MitigationRequestDescriptionWithStatuses ongoingDeleteRequest) {
        String successfulDeleteStatus = MitigationInstanceStatusHelper.getOperationSuccessfulStatus(RequestType.DeleteRequest);
        String inProgressDeleteStatus = MitigationInstanceStatusHelper.getRunningStatus(RequestType.DeleteRequest);
        
        String ongoingDeleteRequestKey = createDeviceAndMitigationNameKey(ongoingDeleteRequest);
        
        if (!mergedMitigations.containsKey(ongoingDeleteRequestKey)) {
            LOG.warn("Expected to see an active mitigation for the ongoing delete request: " + ongoingDeleteRequest);
            mergedMitigations.put(ongoingDeleteRequestKey, Lists.newArrayList(ongoingDeleteRequest));
            return;
        }
        
        // Having more than 1 mitigation for the same mitigation metadata (key formed using deviceName and mitigationName) is an absolute edge-case:
        // Where one of the instance is from the ActiveMitigations query.
        // And we have another instance from an ongoing edit request (whose workflow status is still RUNNING, but some location has finished editing - in which case we add in the
        // edited version of the mitigation for that location in this mergedMitigations data structure, under the same metadata key - since they refer to the same mitigation.
        // However, we don't expect to get more than 2 of such corresponding mitigations - hence the check below to log an error if we ever hit such an unexpected case.
        List<MitigationRequestDescriptionWithStatuses> correspondingMergedMitigations = mergedMitigations.get(ongoingDeleteRequestKey);
        if (correspondingMergedMitigations.size() > 2) {
            LOG.error("Found: " + correspondingMergedMitigations.size() + " mitigations for the mitigation being deleted: " + ReflectionToStringBuilder.toString(ongoingDeleteRequest) +
                      " however, we shouldn't see more than 2 of such at any time.");
        }
        
        for (MitigationInstanceStatus instanceStatus : ongoingDeleteRequest.getInstancesStatusMap().values()) {
            if (instanceStatus.getMitigationStatus().equals(successfulDeleteStatus) || instanceStatus.getMitigationStatus().equals(inProgressDeleteStatus)) {
                for (MitigationRequestDescriptionWithStatuses mergedMitigation : correspondingMergedMitigations) {
                    mergedMitigation.getInstancesStatusMap().put(instanceStatus.getLocation(), instanceStatus);
                    
                    // If there is any instance being deleted for this mitigation, mark the original mitigations' updateJobId to reflect this delete request's jobId.
                    mergedMitigation.getMitigationRequestDescription().setUpdateJobId(ongoingDeleteRequest.getMitigationRequestDescription().getJobId());
                }
            }
        }
    }
    
    /**
     * Helps merging an ongoing edit workflow request with the mitigations already identified as active.
     * The algorithm for merging is as follows:
     * 1. Check if this mitigation being edited shows up in the list of merged mitigations (representing active mitigations). If it doesn't, then we missed fetching this mitigation
     *    when querying for the active mitigations. It is possible, that it might be a timing issue, hence simply log a warn for it and just add this ongoing edit request 
     *    to the mergedMitigations and return.
     * 2. Iterate over each instance's edit operation status:
     *    2.1 If the edit operation is ongoing, then keep a track of the mitigation description in the edit request and this location's status in the editMitigationRequestDescToReturn structure.
     *    2.2 If the edit operation has completed successfully, then keep a track of the mitigation description in the same editMitigationRequestDescToReturn structure mentioned above, but also
     *        remove this location from the mitigation descriptions of the currently active ones.
     *    2.3 If the edit operation has failed, then do nothing, since none of the existing state has changed.
     * @param mergedMitigations Map of String (key formed using deviceName and mitigationName) to a list of MitigationRequestDescriptionWithStatuses instances, identifying mitigations which must be returned back to the caller.
     *                          Note, the List exists in the value only to support the case where for the same key (deviceName and mitigationName), we might have an entry for an active mitigation
     *                          and also an ongoing edit request for that same mitigation.
     * @param ongoingEditRequest An instance of MitigationRequestDescriptionWithStatuses indicating an ongoing edit request.
     */
    private void mergeOngoingEditRequest(Map<String, List<MitigationRequestDescriptionWithStatuses>> mergedMitigations, MitigationRequestDescriptionWithStatuses ongoingEditRequest) {
        String ongoingEditRequestKey = createDeviceAndMitigationNameKey(ongoingEditRequest);
        
        if (!mergedMitigations.containsKey(ongoingEditRequestKey)) {
            LOG.warn("Expected to see an active mitigation for the ongoing edit request: " + ongoingEditRequest);
            mergedMitigations.put(ongoingEditRequestKey, Lists.newArrayList(ongoingEditRequest));
            return;
        }
        
        // Having more than 1 mitigation for the same mitigation metadata (key formed using deviceName and mitigationName) is an absolute edge-case:
        // Where one of the instance is from the ActiveMitigations query.
        // And we have another instance from an ongoing edit request (whose workflow status is still RUNNING, but some location has finished editing - in which case we add in the
        // edited version of the mitigation for that location in this mergedMitigations data structure, under the same metadata key - since they refer to the same mitigation.
        // However, since we are processing an ongoing edit request, we don't expect to have seen any other ongoing edit for the same mitigation. Thus if we see more than
        // 1 of such a mitigation, we log an error.
        List<MitigationRequestDescriptionWithStatuses> correspondingMergedMitigations = mergedMitigations.get(ongoingEditRequestKey);
        if (correspondingMergedMitigations.size() > 1) {
            LOG.error("Found: " + correspondingMergedMitigations.size() + " mitigations for the mitigation being deleted: " + ReflectionToStringBuilder.toString(ongoingEditRequest) +
                      " however, we shouldn't see more than 1 of such at any time.");
        }
        
        MitigationRequestDescriptionWithStatuses editMitigationRequestDescToReturn = new MitigationRequestDescriptionWithStatuses();
        editMitigationRequestDescToReturn.setMitigationRequestDescription(ongoingEditRequest.getMitigationRequestDescription());
        editMitigationRequestDescToReturn.setInstancesStatusMap(new HashMap<String, MitigationInstanceStatus>());
        
        String editSuccessStatus = MitigationInstanceStatusHelper.getOperationSuccessfulStatus(RequestType.EditRequest);
        String editInProgressStatus = MitigationInstanceStatusHelper.getRunningStatus(RequestType.EditRequest);
        
        // Flag to indicate if whether the requests which are being edited have been updated to have its updatedJobId set to the jobId of this edit request.
        boolean originalRequestUpdated = false;
        
        for (MitigationInstanceStatus instanceStatus : ongoingEditRequest.getInstancesStatusMap().values()) {
            if (instanceStatus.getMitigationStatus().equals(editSuccessStatus)) {
                for (MitigationRequestDescriptionWithStatuses mergedMitigation : correspondingMergedMitigations) {
                    mergedMitigation.getInstancesStatusMap().remove(instanceStatus.getLocation());
                }
                editMitigationRequestDescToReturn.getInstancesStatusMap().put(instanceStatus.getLocation(), instanceStatus);
            }
            
            if (instanceStatus.getMitigationStatus().equals(editSuccessStatus) || instanceStatus.getMitigationStatus().equals(editInProgressStatus)) {
                editMitigationRequestDescToReturn.getInstancesStatusMap().put(instanceStatus.getLocation(), instanceStatus);
                
                // If there is any instance being edited for this mitigation, mark the original mitigations' updateJobId to reflect this edit request's jobId.
                if (!originalRequestUpdated) {
                    for (MitigationRequestDescriptionWithStatuses originalMitigation : correspondingMergedMitigations) {
                        originalMitigation.getMitigationRequestDescription().setUpdateJobId(ongoingEditRequest.getMitigationRequestDescription().getJobId());
                    }
                    originalRequestUpdated = true;
                }
            }
        }
        
        if (editMitigationRequestDescToReturn.getInstancesStatusMap().size() > 0) {
            mergedMitigations.get(ongoingEditRequestKey).add(editMitigationRequestDescToReturn);
        }
    }
    
    /**
     * A simple helper method to generate a string using the deviceName and mitigationName.
     * @param mitigation MitigationRequestDescriptionWithStatuses instance to use to generate the deviceAndMitigationName key.
     * @return String representing a key combining the deviceName and mitigationName
     */
    public static String createDeviceAndMitigationNameKey(MitigationRequestDescriptionWithStatuses mitigation) {
        return mitigation.getMitigationRequestDescription().getDeviceName() + KEY_SEPARATOR + mitigation.getMitigationRequestDescription().getMitigationName();
    }
}
