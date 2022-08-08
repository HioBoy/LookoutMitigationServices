package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import lombok.AllArgsConstructor;
import lombok.NonNull;

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
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;


@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class UpdateBlackWatchMitigationRegionalCellPlacementActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(UpdateBlackWatchMitigationRegionalCellPlacementActivity.class);

    private enum UpdateBlackWatchMitigationRegionalCellPlacementExceptions {
        BadRequest, InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the
    // ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections
            .unmodifiableSet(Sets.newHashSet(
                    UpdateBlackWatchMitigationRegionalCellPlacementExceptions.BadRequest.name(),
                    UpdateBlackWatchMitigationRegionalCellPlacementExceptions.InternalError.name()));

    @NonNull
    private final RequestValidator requestValidator;

    @NonNull
    private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;

    @Validated
    @Operation("UpdateBlackWatchMitigationRegionalCellPlacement")
    @Documentation("UpdateBlackWatchMitigationRegionalCellPlacement")
    public @NonNull UpdateBlackWatchMitigationRegionalCellPlacementResponse enact(
            @NonNull UpdateBlackWatchMitigationRegionalCellPlacementRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(),
                "UpdateBlackWatchMitigationRegionalCellPlacement.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.info(String
                    .format("UpdateBlackWatchMitigationRegionalCellPlacementActivity " +
                                    "called with RequestId: %s and request: %s.",
                            requestId,
                            ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            requestValidator.validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(request);

            UpdateBlackWatchMitigationRegionalCellPlacementResponse response =
                    blackwatchMitigationInfoHandler.updateBlackWatchMitigationRegionalCellPlacement(
                            request.getMitigationId(),
                            request.getCellNames(),
                            getIdentity().getAttribute(Identity.AWS_USER_ARN),
                            tsdMetrics);

            response.setRequestId(requestId);

            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format(
                    ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId,
                    "UpdateBlackWatchMitigationRegionalCellPlacementActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + UpdateBlackWatchMitigationRegionalCellPlacementExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in UpdateBlackWatchMitigationRegionalCellPlacementActivity for requestId: "
                    + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request "
                    + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + UpdateBlackWatchMitigationRegionalCellPlacementExceptions.InternalError.name(), 1);
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
