package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.service.Identity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class UpdateBlackWatchMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(UpdateBlackWatchMitigationActivity.class);

    private enum UpdateBlackWatchMitigationExceptions {
        BadRequest, InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the
    // ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections
            .unmodifiableSet(Sets.newHashSet(
                    UpdateBlackWatchMitigationExceptions.BadRequest.name(),
                    UpdateBlackWatchMitigationExceptions.InternalError.name()));

    @NonNull
    private final RequestValidator requestValidator;
    @NonNull
    private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;

    @Validated
    @Operation("UpdateBlackWatchMitigation")
    @Documentation("UpdateBlackWatchMitigation")
    public @NonNull UpdateBlackWatchMitigationResponse enact(
            @NonNull UpdateBlackWatchMitigationRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(),
                "UpdateBlackWatchMitigation.enact");
        
        String userARN = getIdentity().getAttribute(Identity.AWS_USER_ARN);

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            LOG.info(String
                    .format("UpdateBlackWatchMitigationActivity called with RequestId: %s and request: %s.",
                            requestId,
                            ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            String mitigationId = request.getMitigationId();
            Integer minsToLive = request.getMinutesToLive();
            MitigationActionMetadata metadata = request.getMitigationActionMetadata();
            
            // Get the current MitigationSettingsJSON from the mitigation state table.
            // If the mitigation doesn't exist we will validate the request anyway and
            // fail when we try to update.
            String currentJson;
            try {
                currentJson = blackwatchMitigationInfoHandler
                    .getMitigationState(mitigationId)
                    .getMitigationSettingsJSON();
            } catch (NullPointerException e) {
                currentJson = null;
            }
            BlackWatchTargetConfig currentTargetConfig = RequestValidator.parseMitigationSettingsJSON(
                    currentJson);

            // Merge PPS & BPS, then validate
            BlackWatchTargetConfig targetConfig = requestValidator.validateUpdateBlackWatchMitigationRequest(
                    request, currentTargetConfig, userARN);

            UpdateBlackWatchMitigationResponse response = blackwatchMitigationInfoHandler.updateBlackWatchMitigation(
                    mitigationId, minsToLive, metadata, targetConfig, userARN, tsdMetrics);
            
            response.setRequestId(requestId);
            return response;

        } catch (IllegalArgumentException ex) {
            String msg = String.format(
                    ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, 
                    "UpdateBlackWatchMitigationActivity",  ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + UpdateBlackWatchMitigationExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in UpdateBlackWatchMitigationActivity for requestId: "
                    + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request "
                            + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + UpdateBlackWatchMitigationExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS,
                    requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE,
                    requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

