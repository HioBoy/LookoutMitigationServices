package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchLocationsResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
/**
 * Get all BlackWatch Locations with their AdminIn state, change reason, change time, change user and InService state.
 * */
@Service("LookoutMitigationService")
public class ListBlackWatchLocationsActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListBlackWatchLocationsActivity.class);

    private enum ListBlackWatchLocationsExceptions {
        BadRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(ListBlackWatchLocationsExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet())); 

    private final RequestValidator requestValidator;
    private final LocationStateInfoHandler locationStateInfoHandler;

    @ConstructorProperties({"requestValidator", "locationStateInfoHandler"})
    public ListBlackWatchLocationsActivity(@NonNull RequestValidator requestValidator,
            @NonNull LocationStateInfoHandler locationStateInfoHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(locationStateInfoHandler);
        this.locationStateInfoHandler = locationStateInfoHandler;
    }

    @Validated
    @Operation("ListBlackWatchLocations")
    @Documentation("ListBlackWatchLocations")
    public @NonNull ListBlackWatchLocationsResponse enact(@NonNull ListBlackWatchLocationsRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ListBlackWatchLocations.enact");
        try {            
            LOG.info(String.format("ListBlackWatchLocationsActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            String region = request.getRegion();
            
            // Step 1. Validate this request
            requestValidator.validateListBlackWatchLocationsRequest(request);
            
            // Step 2. Fetch list of BlackWatch locations and their AdminIn state
            List<BlackWatchLocation> listOfLocationsAndAdminInState = locationStateInfoHandler.getBlackWatchLocation(region, tsdMetrics);

            // Step 3. Create the response object to return back to the client.
            ListBlackWatchLocationsResponse response = new ListBlackWatchLocationsResponse();
            response.setListOfLocationsAndAdminState(listOfLocationsAndAdminInState);
            response.setRequestId(requestId);

            requestSuccessfullyProcessed = true;
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "ListBlackWatchLocationsActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListBlackWatchLocationsExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in ListBlackWatchLocationsActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListBlackWatchLocationsExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
