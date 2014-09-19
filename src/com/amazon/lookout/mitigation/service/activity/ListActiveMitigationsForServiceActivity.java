package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.google.common.collect.Lists;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

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
    
    private final RequestValidator requestValidator;
    private final ActiveMitigationInfoHandler activeMitigationInfoHandler;
    private final RequestInfoHandler requestInfoHandler;
    
    public static final String KEY_SEPARATOR = "#";
    
    @ConstructorProperties({"requestValidator", "activeMitigationInfoHandler", "requestInfoHandler"})
    public ListActiveMitigationsForServiceActivity(@Nonnull RequestValidator requestValidator, @Nonnull ActiveMitigationInfoHandler activeMitigationInfoHandler,
            @Nonnull RequestInfoHandler requestInfoHandler) {
        
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(activeMitigationInfoHandler);
        this.activeMitigationInfoHandler = activeMitigationInfoHandler;
        
        Validate.notNull(requestInfoHandler);
        this.requestInfoHandler = requestInfoHandler;
        
    }
    
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
            
            // Step 2. Get a list of ActiveMitigationDetails. This will contain the deviceName, location, jobId and mitigationName. The information
            // that is gotten from this step will be used to query the MITIGATION_REQUESTS table for the remaining information needed for our response.
            List<ActiveMitigationDetails> listOfActiveMitigationDetails = 
                    activeMitigationInfoHandler.getActiveMitigationsForService(request.getServiceName(), request.getDeviceName(), request.getLocations(), tsdMetrics);
            
            // Create a map of deviceName and jobId to MitigationRequestDescription. We want to condense the information within the ACTIVE_MITIGATIONS
            // table based on deviceName and jobId so that we only query the request table for each unique jobId. Additionally we 
            // also aggregate all of the locations for each item in the table with a unique deviceName and jobId combination into a list that 
            // will be set within each MitigationRequestDescription that this key maps to. 
            Map<String, MitigationRequestDescriptionWithLocations> descriptionsWithLocationsMap = new HashMap<>();
            // Step 3. Iterate through the list of ActiveMitigationDetails and use the deviceName and jobId to get the remaining information needed 
            // for our response.
            for (ActiveMitigationDetails activeMitigationDetails : listOfActiveMitigationDetails) {
                String deviceName = activeMitigationDetails.getDeviceName();
                long jobId = activeMitigationDetails.getJobId();
                String location = activeMitigationDetails.getLocation();
                final String key = deviceName + KEY_SEPARATOR + jobId;
                if (!descriptionsWithLocationsMap.containsKey(key)) {
                    // Step 4. Set all of the acquired information within a MitigationRequestDescription and then pass that to the list of MitigationRequestDescriptions.
                    MitigationRequestDescription mitigationDescription = requestInfoHandler.getMitigationRequestDescription(deviceName, jobId, tsdMetrics);
                    
                    MitigationRequestDescriptionWithLocations descriptionWithLocations = new MitigationRequestDescriptionWithLocations();
                    descriptionWithLocations.setMitigationRequestDescription(mitigationDescription);
                    descriptionWithLocations.setLocations(Lists.newArrayList(location));
                    
                    descriptionsWithLocationsMap.put(key, descriptionWithLocations);
                } else {
                    descriptionsWithLocationsMap.get(key).getLocations().add(location);
                }
            }
            
            // Step 5. Get a list of mitigation descriptions for requests which are being worked on right now.
            List<MitigationRequestDescriptionWithLocations> ongoingRequestsDescription = 
                                                        requestInfoHandler.getInProgressRequestsDescription(request.getServiceName(), request.getDeviceName(), tsdMetrics);
            
            // Step 6. Some of the ongoing requests might have been captured from the ActiveMitigations table, filter those out from this list, 
            // else add the ongoing request to the descriptionsWithLocationsMap structure.
            mergeOngoingRequestsToActiveOnes(ongoingRequestsDescription, descriptionsWithLocationsMap);
            
            // Create a List using the values from mitigationDescriptions
            List<MitigationRequestDescriptionWithLocations> requestDescriptionsWithLocations = new ArrayList<>(descriptionsWithLocationsMap.values());
            ListActiveMitigationsForServiceResponse response = new ListActiveMitigationsForServiceResponse();
            response.setServiceName(request.getServiceName());
            response.setMitigationRequestDescriptionsWithLocations(requestDescriptionsWithLocations);
            LOG.info(String.format("ListMitigationsActivity called with RequestId: %s returned: %s.", requestId, ReflectionToStringBuilder.toString(response)));  
            
            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in ListActiveMitigationsForServiceActivity for requestId: " + requestId + ", reason: " + ex.getMessage() + 
                                       " for request: " + ReflectionToStringBuilder.toString(request));
            LOG.warn(msg, ex);
            tsdMetrics.addCount(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListActiveMitigationsExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for ListActiveMitigationsForServiceActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(request));
            LOG.error(msg, internalError);
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
     * Helps merge ongoing requests to the currently active requests. If a request is marked as ongoing (since the WorkflowStatus is Running), but the 
     * tasks at a location has already completed, then it would show up in the currently active ones - hence in such cases we only add the locations
     * which haven't yet completed to the MitigationRequestDescriptionWithLocations, to avoid having the loactions with active mitigations listed twice.
     * @param ongoingRequestsWithLocations List of MitigationRequestDescriptionWithLocations instances, each instance defining an ongoing request along with the locations where this request is running.
     * @param descriptionsWithLocationsMap Map of active mitigations - with generated key of deviceName+Separator+jobId and value as an instance of MitigationRequestDescriptionWithLocations.
     */
    protected void mergeOngoingRequestsToActiveOnes(@Nonnull List<MitigationRequestDescriptionWithLocations> ongoingRequestsWithLocations, 
                                                    @Nonnull Map<String, MitigationRequestDescriptionWithLocations> descriptionsWithLocationsMap) {
        Validate.notNull(ongoingRequestsWithLocations);
        Validate.notNull(descriptionsWithLocationsMap);
        
        for (MitigationRequestDescriptionWithLocations descriptionWithLocations : ongoingRequestsWithLocations) {
            MitigationRequestDescription description = descriptionWithLocations.getMitigationRequestDescription();
            final String key = description.getDeviceName() + KEY_SEPARATOR + description.getJobId();
            
            if (!descriptionsWithLocationsMap.containsKey(key)) {
                descriptionsWithLocationsMap.put(key, descriptionWithLocations);
            } else {
                // If there is an entry for this mitigation in the existing map of descriptionsWithLocationsMap, check if there are any missing locations and add them in,
                // since the workflow is still running for this location.
                List<String> locationsForExistingEntry = descriptionsWithLocationsMap.get(key).getLocations();
                if (!locationsForExistingEntry.containsAll(descriptionWithLocations.getLocations())) {
                    for (String location : descriptionWithLocations.getLocations()) {
                        if (!locationsForExistingEntry.contains(location)) {
                            locationsForExistingEntry.add(location);
                        }
                    }
                }
            }
        }
    }
}
