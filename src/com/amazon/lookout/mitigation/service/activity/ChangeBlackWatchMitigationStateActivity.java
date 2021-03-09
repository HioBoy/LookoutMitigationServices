package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationStateRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationStateResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ThreadSafe
@Service("LookoutMitigationService")
@AllArgsConstructor
public class ChangeBlackWatchMitigationStateActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ChangeBlackWatchMitigationStateActivity.class);

    private enum ActivityExceptions {
        BadRequest,
        InternalError
    }

    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Stream.of(ActivityExceptions.values())
                    .map(e -> e.name())
                    .collect(Collectors.toSet()));

    @NonNull private final RequestValidator requestValidator;
    @NonNull private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;

    @Validated
    @Operation("ChangeBlackWatchMitigationState")
    @Documentation("ChangeBlackWatchMitigationState")
    public @NonNull
    ChangeBlackWatchMitigationStateResponse enact(@NonNull ChangeBlackWatchMitigationStateRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ChangeBlackWatchMitigationState.enact");
        try {
            LOG.info(String.format(getClass().getName() + " called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            String mitigationId = request.getMitigationId();
            // Validate this request
            requestValidator.validateChangeBlackWatchMitigationStateRequest(request);

            // change mitigation state
            MitigationState.State expectedState = MitigationState.State.valueOf(request.getExpectedState());
            MitigationState.State newState = MitigationState.State.valueOf(request.getNewState());
            blackwatchMitigationInfoHandler.changeMitigationState(mitigationId, expectedState, newState, request.getMitigationActionMetadata());

            // Create the response object to return back to the client.
            ChangeBlackWatchMitigationStateResponse response = new ChangeBlackWatchMitigationStateResponse();
            response.setRequestId(requestId);
            response.setMitigationId(mitigationId);

            requestSuccessfullyProcessed = true;
            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT,
                    requestId, getClass().getName(), ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ActivityExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in " + getClass().getName() +
                    " for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ActivityExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
