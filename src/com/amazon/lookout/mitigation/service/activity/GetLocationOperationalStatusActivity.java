package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.*;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.HostStatusInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import lombok.NonNull;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Get operational status of a specific location.
 * */
@Service("LookoutMitigationService")
public class GetLocationOperationalStatusActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetLocationOperationalStatusActivity.class);

    private enum GetLocationHostStatusExceptions {
        BadRequest,
        InternalError
    }

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
    public GetLocationOperationalStatusActivity(@NonNull RequestValidator requestValidator,
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
    @Operation("GetLocationOperationalStatus")
    @Documentation("GetLocationOperationalStatus")
    public @NonNull GetLocationOperationalStatusResponse enact(@NonNull GetLocationOperationalStatusRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetLocationHostStatus.enact");
        try {
            LOG.info(String.format("GetLocationHostStatusActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step 1. Validate this request
            requestValidator.validateGetLocationOperationalStatusRequest(request);

            String location = request.getLocation().toLowerCase();
            LocationState locationState = locationStateInfoHandler.getLocationState(location, tsdMetrics);

            // Checking if location requested exists
            if(locationState == null) {
                throw new IllegalArgumentException("Location: " + location + " does not exists");
            }

            // Step 2. Create the response object to return back to the client.
            GetLocationOperationalStatusResponse response = new GetLocationOperationalStatusResponse();
            response.setRequestId(requestId);
            response.setIsOperational(locationStateInfoHandler.checkIfLocationIsOperational(location, tsdMetrics));
            response.setPendingOperations(locationStateInfoHandler.getPendingOperationLocks(location, tsdMetrics));
            response.setOperationChanges(locationStateInfoHandler.getOperationChanges(location, tsdMetrics));

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
