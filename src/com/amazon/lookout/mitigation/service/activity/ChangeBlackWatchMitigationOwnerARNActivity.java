package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.NonNull;
import lombok.AllArgsConstructor;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNRequest;
import com.amazon.lookout.mitigation.service.ChangeBlackWatchMitigationOwnerARNResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;

@ThreadSafe
@Service("LookoutMitigationService")
@AllArgsConstructor
public class ChangeBlackWatchMitigationOwnerARNActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ChangeBlackWatchMitigationOwnerARNActivity.class);

    private enum ActivityExceptions {
        BadRequest,
        InternalError
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Stream.of(ActivityExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet())); 
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;
    
    @Validated
    @Operation("ChangeBlackWatchMitigationOwnerARN")
    @Documentation("ChangeBlackWatchMitigationOwnerARN")
    public @NonNull ChangeBlackWatchMitigationOwnerARNResponse enact(@NonNull ChangeBlackWatchMitigationOwnerARNRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ChangeBlackWatchMitigationOwnerARN.enact");
        try {
            LOG.info(String.format(getClass().getName() + " called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            String mitigationId = request.getMitigationId();
            // Validate this request
            requestValidator.validateChangeBlackWatchMitigationOwnerARNRequest(request);

            // Deactivate
            blackwatchMitigationInfoHandler.changeOwnerARN(mitigationId,
                    request.getNewOwnerARN(), request.getExpectedOwnerARN(),
                    request.getMitigationActionMetadata());

            // Create the response object to return back to the client.
            ChangeBlackWatchMitigationOwnerARNResponse response = new ChangeBlackWatchMitigationOwnerARNResponse();
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
