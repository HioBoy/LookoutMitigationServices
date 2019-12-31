package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazon.lookout.mitigation.datastore.LocationStateLocksDAO;
import com.amazon.lookout.mitigation.service.activity.validator.HostnameValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
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
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchLocationStateResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
/**
 * Update AdminIn state for the given location with a reason.
 * */
@Service("LookoutMitigationService")
public class UpdateBlackWatchLocationStateActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(UpdateBlackWatchLocationStateActivity.class);

    private enum UpdateBlackWatchLocationStateExceptions {
        BadRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(UpdateBlackWatchLocationStateExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));

    private final RequestValidator requestValidator;
    private final LocationStateInfoHandler locationStateInfoHandler;
    private final LocationStateLocksDAO locationStateLocksDAO;

    @ConstructorProperties({"requestValidator", "locationStateInfoHandler", "locationStateLocksDAO"})
    public UpdateBlackWatchLocationStateActivity(@NonNull RequestValidator requestValidator,
                                                 @NonNull LocationStateInfoHandler locationStateInfoHandler,
                                                 @NonNull LocationStateLocksDAO locationStateLocksDAO) {

        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(locationStateInfoHandler);
        this.locationStateInfoHandler = locationStateInfoHandler;

        Validate.notNull(locationStateLocksDAO);
        this.locationStateLocksDAO = locationStateLocksDAO;
    }

    @Validated
    @Operation("UpdateBlackWatchLocationState")
    @Documentation("UpdateBlackWatchLocationState")
    public @NonNull UpdateBlackWatchLocationStateResponse enact(@NonNull UpdateBlackWatchLocationStateRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "UpdateBlackWatchLocationState.enact");
        try {            
            LOG.info(String.format("UpdateBlackWatchLocationStateActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step 1. Validate this request
            requestValidator.validateUpdateBlackWatchLocationStateRequest(request);

            if (locationStateLocksDAO.acquireWriterLock(DeviceName.BLACKWATCH_BORDER, request.getLocation())) {
                String location = request.getLocation().toLowerCase();
                String reason = request.getReason();
                boolean adminIn = request.isAdminIn();
                String locationType = request.getLocationType();
                String changeId = request.getChangeId();
                String operationId = request.getOperationId();
                //overrideLocks is set to true because this API is exposed only to BLACKWATCH developers for forced operation
                boolean overrideLocks = true;

                // Step 2. Update AdminIn state for this location
                locationStateInfoHandler.updateBlackWatchLocationAdminIn(location, adminIn, reason, locationType, operationId, changeId, overrideLocks, tsdMetrics);
            }
            // Step 3. Create the response object to return back to the client.
            UpdateBlackWatchLocationStateResponse response = new UpdateBlackWatchLocationStateResponse();
            response.setRequestId(requestId);

            requestSuccessfullyProcessed = true;
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "UpdateBlackWatchLocationStateActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + UpdateBlackWatchLocationStateExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in UpdateBlackWatchLocationStateActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + UpdateBlackWatchLocationStateExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            locationStateLocksDAO.releaseWriterLock(DeviceName.BLACKWATCH_BORDER, request.getLocation());
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}