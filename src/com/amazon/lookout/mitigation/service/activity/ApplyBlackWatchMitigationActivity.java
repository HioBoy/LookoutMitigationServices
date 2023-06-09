package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;
import java.util.Set;

import com.amazon.coral.metrics.MetricsFactory;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
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
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.MitigationNotOwnedByRequestor400;
import com.amazon.lookout.mitigation.service.MitigationLimitByOwnerExceeded400;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ApplyBlackWatchMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ApplyBlackWatchMitigationActivity.class);

    private enum ApplyBlackWatchMitigationExceptions {
        BadRequest, InternalError, LimitExceeded,
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the
    // ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections
            .unmodifiableSet(Sets.newHashSet(
                    ApplyBlackWatchMitigationExceptions.BadRequest.name(),
                    ApplyBlackWatchMitigationExceptions.InternalError.name()));

    @NonNull
    private final RequestValidator requestValidator;

    @NonNull
    private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;

    private final boolean regionalMitigationsEnabled;

    @NonNull
    private final MetricsFactory metricsFactory;

    public ApplyBlackWatchMitigationActivity(RequestValidator requestValidator, BlackWatchMitigationInfoHandler
        blackwatchMitigationInfoHandler) {
        this.requestValidator = requestValidator;
        this.blackwatchMitigationInfoHandler = blackwatchMitigationInfoHandler;
        this.regionalMitigationsEnabled = false;
        this.metricsFactory = null;
    }

    @Validated
    @Operation("ApplyBlackWatchMitigation")
    @Documentation("ApplyBlackWatchMitigation")
    public @NonNull ApplyBlackWatchMitigationResponse enact(@NonNull ApplyBlackWatchMitigationRequest request) {

        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ApplyBlackWatchMitigation.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            String userARN = getIdentity().getAttribute(Identity.AWS_USER_ARN);
            LOG.info(String.format("ApplyBlackWatchMitigationActivity called with RequestId: %s and request: %s "
                    + "and userARN:%s.", requestId, ReflectionToStringBuilder.toString(request), userARN));

            // Step 1. Validate your request.
            BlackWatchTargetConfig targetConfig = requestValidator.validateApplyBlackWatchMitigationRequest(request,
                    userARN);

            // placement_tags is a new field in mitigation JSON that will control placing mitigations to
            // BlackWatch in Region (BWIR). BWIR hasn't been enabled in all regions yet.
            // placement_tags field exists as part of the BlackWatchTargetConfig model to allow BWIR
            // enablement in BlackWatch API Worker independently of BWIR enablement in BlackWatch API
            // Coral activities. It prevents Worker from failing with an "Unrecognized field" error on
            // deserialization of mitigation state.
            if (targetConfig.getGlobal_deployment() != null &&
                    targetConfig.getGlobal_deployment().getPlacement_tags() != null &&
                    !regionalMitigationsEnabled) {
                throw new IllegalArgumentException("placement_tags are not supported yet.");
            }

            String resourceId = request.getResourceId();
            String resourceType = request.getResourceType();
            Integer minsToLive = request.getMinutesToLive();
            MitigationActionMetadata metadata = request.getMitigationActionMetadata();
            boolean allowAutoMitigationOverride = request.isAllowAutoMitigationOverride();
            boolean bypassConfigValidations = request.isBypassConfigValidations();

            ApplyBlackWatchMitigationResponse response = blackwatchMitigationInfoHandler
                    .applyBlackWatchMitigation(resourceId, resourceType, minsToLive,
                            metadata, targetConfig, userARN, tsdMetrics, allowAutoMitigationOverride, bypassConfigValidations);
            response.setRequestId(requestId);

            return response;

        } catch (MitigationNotOwnedByRequestor400 ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT,
                    requestId, "ApplyBlackWatchMitigationActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + ApplyBlackWatchMitigationExceptions.BadRequest.name(), 1);
            throw new MitigationNotOwnedByRequestor400(msg);
        } catch (IllegalArgumentException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT,
                    requestId, "ApplyBlackWatchMitigationActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + ApplyBlackWatchMitigationExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (MitigationLimitByOwnerExceeded400 limitError) {
            String msg = "Limit Exceeded Error in ApplyBlackWatchMitigationActivity for requestId: "
                    + requestId + ", reason: " + limitError.getMessage();
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + ApplyBlackWatchMitigationExceptions.LimitExceeded.name(), 1);
            throw new MitigationLimitByOwnerExceeded400(msg, limitError);
        } catch (Exception internalError) {
            String msg = "Internal error in ApplyBlackWatchMitigationActivity for requestId: "
                    + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX 
                    + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX 
                    + ApplyBlackWatchMitigationExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg, internalError);
        } finally {
            tsdMetrics.addCount(
                    LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(
                    LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }

}

