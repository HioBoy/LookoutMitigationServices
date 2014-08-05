package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.activities.model.MitigationMetadata;

@ThreadSafe
@Service("LookoutMitigationService")
public class ListActiveMitigationsForServiceActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListActiveMitigationsForServiceActivity.class);
    
    private final RequestValidator requestValidator;
    private final ActiveMitigationInfoHandler activeMitigationInfoHandler;
    private final RequestInfoHandler requestInfoHandler;
    
    private final String KEY_SEPARATOR = "#";
    
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
            // Step 1. Validate your request.
            requestValidator.validateListActiveMitigationsForServiceRequest(request);
            // Step 2. Get a list of ActiveMitigationDetails. This will contain the deviceName, location, jobId and mitigationName. The information
            // that is gotten from this step will be used to query the MITIGATION_REQUESTS table for the remaining information needed for our response.
            List<ActiveMitigationDetails> listOfActiveMitigationDetails = activeMitigationInfoHandler.getActiveMitigationsForService(request.getServiceName(),
                    request.getDeviceName(), request.getLocations(), tsdMetrics);
            
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
                    // Store the request data for a given mitigation in a RequestInfo object. The data contained here will be joined
                    // with the data contained in the ActiveMitigationDetails object to get the full picture of an active mitigation.
                    MitigationMetadata mitigationMetadata = requestInfoHandler.getMitigationMetadata(deviceName, jobId, tsdMetrics);
                    // Step 4. Set all of the acquired information within a MitigationRequestDescription and then pass that to the list of
                    // MitigationRequestDescriptions.
                    MitigationRequestDescription mitigationRequestDescription = new MitigationRequestDescription();
                    mitigationRequestDescription.setDeviceName(deviceName);
                    mitigationRequestDescription.setMitigationName(activeMitigationDetails.getMitigationName());
                    mitigationRequestDescription.setMitigationVersion(activeMitigationDetails.getMitigationVersion());
                    mitigationRequestDescription.setMitigationActionMetadata(mitigationMetadata.getMitigationActionMetadata());
                    mitigationRequestDescription.setMitigationDefinition(mitigationMetadata.getMitigationDefinition());
                    mitigationRequestDescription.setMitigationTemplate(mitigationMetadata.getMitigationTemplate());
                    mitigationRequestDescription.setRequestDate(mitigationMetadata.getRequestDate());

                    MitigationRequestDescriptionWithLocations descriptionWithLocations = new MitigationRequestDescriptionWithLocations();
                    descriptionWithLocations.setMitigationRequestDescription(mitigationRequestDescription);
                    descriptionWithLocations.setLocations(Lists.newArrayList(location));
                    
                    descriptionsWithLocationsMap.put(key, descriptionWithLocations);
                } else {
                    descriptionsWithLocationsMap.get(key).getLocations().add(location);
                }
            }
            
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
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for ListActiveMitigationsForServiceActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(request));
            LOG.error(msg, internalError);
            requestSuccessfullyProcessed = false;
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
