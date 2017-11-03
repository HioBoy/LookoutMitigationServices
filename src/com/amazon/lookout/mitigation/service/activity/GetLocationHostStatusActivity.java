package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
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
import com.amazon.lookout.mitigation.service.GetLocationHostStatusRequest;
import com.amazon.lookout.mitigation.service.GetLocationHostStatusResponse;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.HostStatusInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
/**
 * Get all hosts status on a specific location.
 * */
@Service("LookoutMitigationService")
public class GetLocationHostStatusActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetLocationHostStatusActivity.class);

    private enum GetLocationHostStatusExceptions {
        BadRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetLocationHostStatusExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet())); 

    private final RequestValidator requestValidator;
    private final HostStatusInfoHandler hostStatusHandler;
    private final LocationStateInfoHandler locationStateInfoHandler;

    @ConstructorProperties({"requestValidator", "hostStatusHandler", "locationStateInfoHandler"})
    public GetLocationHostStatusActivity(@NonNull RequestValidator requestValidator,
            @NonNull HostStatusInfoHandler hostStatusHandler,
            @NonNull LocationStateInfoHandler locationStateInfoHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(hostStatusHandler);
        this.hostStatusHandler = hostStatusHandler;

        Validate.notNull(locationStateInfoHandler);
        this.locationStateInfoHandler = locationStateInfoHandler;
    }

    @Validated
    @Operation("GetLocationHostStatus")
    @Documentation("GetLocationHostStatus")
    public @NonNull GetLocationHostStatusResponse enact(@NonNull GetLocationHostStatusRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetLocationHostStatus.enact");
        try {            
            LOG.info(String.format("GetLocationHostStatusActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step 1. Validate this request
            requestValidator.validateGetLocationHostStatusRequest(request);

            // Step 2. Fetch state for location
            String location = request.getLocation().toLowerCase();
            LocationState locationState;
            try {
                locationState = locationStateInfoHandler.getLocationState(location, tsdMetrics);
            } catch (Exception ex) {
                LOG.warn(String.format("Failed to get location state for host status in location: %s", location));
                locationState = LocationState.builder().locationName(location).build();
            }

            // Step 2. Fetch list of hosts statuses on this location
            List<HostStatusInLocation> listOfHostStatusesInLocation = hostStatusHandler.getHostsStatus(locationState, tsdMetrics);

            // Step 3. Create the response object to return back to the client.
            GetLocationHostStatusResponse response = new GetLocationHostStatusResponse();
            response.setLocation(location);
            response.setLocationState(locationStateInfoHandler.convertLocationState(locationState));
            response.setListOfHostStatusesInLocation(listOfHostStatusesInLocation);
            response.setRequestId(requestId);

            requestSuccessfullyProcessed = true;
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetLocationHostStatusActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetLocationHostStatusExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in GetLocationHostStatusActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetLocationHostStatusExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
