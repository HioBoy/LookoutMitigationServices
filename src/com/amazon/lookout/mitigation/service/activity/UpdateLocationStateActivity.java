package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.datastore.LocationStateLocksDAO;
import com.amazon.lookout.mitigation.service.*;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.HostnameValidator;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import lombok.NonNull;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.ConstructorProperties;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Update AdminIn state for the given location with a changeId and OperationId.
 */
@Service("LookoutMitigationService")
public class UpdateLocationStateActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(UpdateLocationStateActivity.class);

    private enum UpdateBlackWatchLocationStateExceptions {
        BadRequest,
        InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(UpdateBlackWatchLocationStateExceptions.values())
                    .map(e -> e.name())
                    .collect(Collectors.toSet()));

    private final RequestValidator requestValidator;
    private final LocationStateInfoHandler locationStateInfoHandler;
    private final HostnameValidator hostNameValidator;
    private final LocationStateLocksDAO locationStateLocksDAO;

    @ConstructorProperties({"requestValidator", "locationStateInfoHandler", "hostNameValidator", "locationStateLocksDAO"})
    public UpdateLocationStateActivity(@NonNull RequestValidator requestValidator,
                                       @NonNull LocationStateInfoHandler locationStateInfoHandler,
                                       @NonNull HostnameValidator hostNameValidator,
                                       @NonNull LocationStateLocksDAO locationStateLocksDAO) {

        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(locationStateInfoHandler);
        this.locationStateInfoHandler = locationStateInfoHandler;

        Validate.notNull(hostNameValidator);
        this.hostNameValidator = hostNameValidator;

        Validate.notNull(locationStateLocksDAO);
        this.locationStateLocksDAO = locationStateLocksDAO;
    }

    @Validated
    @Operation("UpdateLocationState")
    @Documentation("UpdateLocationState")
    public @NonNull
    UpdateLocationStateResponse enact(@NonNull UpdateLocationStateRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "UpdateLocationStateRequest.enact");
        try {
            LOG.info(String.format("UpdateLocationStateRequest called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            String operationId = request.getOperationId();
            String changeId = request.getChangeId();
            boolean adminIn = request.isAdminIn();
            String location = request.getLocation().toLowerCase();
            //overrideLocks is set to false because this API is exposed externally and should not be allowed forced operation
            boolean overrideLocks = false;

            // Step 1. Validate this request
            requestValidator.validateUpdateLocationStateRequest(request);

            // Checking if location requested exists
            if(locationStateInfoHandler.getLocationState(location, tsdMetrics) == null) {
                throw new IllegalArgumentException("Location: " + location + " does not exists");
            }

            // Step 2. Creating lock to avoid any concurrent updates
            if (locationStateLocksDAO.acquireWriterLock(DeviceName.BLACKWATCH_BORDER, request.getLocation())) {

                // Step 3. Validate if all other stacks are in service and only applicable if setting adminIn = false
                if (!adminIn && !locationStateInfoHandler.validateOtherStacksInService(location, tsdMetrics)) {
                    String msg = String.format("Other stacks not in service for location %s", location);
                    throw new IllegalStateException(msg);
                }

                // Step 4. Update AdminIn state for this location
                locationStateInfoHandler.updateBlackWatchLocationAdminIn(location, adminIn, changeId, null, operationId, changeId, overrideLocks, tsdMetrics);
            }

            // Step 6. Create the response object to return back to the client.
            UpdateLocationStateResponse response = new UpdateLocationStateResponse();
            response.setRequestId(requestId);
            response.setPendingOperations(locationStateInfoHandler.getPendingOperationLocks(location, tsdMetrics));

            requestSuccessfullyProcessed = true;
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "UpdateLocationStateActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + UpdateBlackWatchLocationStateExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in UpdateLocationStateActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + UpdateBlackWatchLocationStateExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            // Step 7. Release lock
            locationStateLocksDAO.releaseWriterLock(DeviceName.BLACKWATCH_BORDER, request.getLocation());
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}