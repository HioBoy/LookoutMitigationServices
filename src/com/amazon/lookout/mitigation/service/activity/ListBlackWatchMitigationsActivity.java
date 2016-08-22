package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
import com.amazon.lookout.mitigation.service.BlackWatchMitigationDefinition;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsRequest;
import com.amazon.lookout.mitigation.service.ListBlackWatchMitigationsResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ListBlackWatchMitigationsActivity extends Activity {
    private static final Log LOG = LogFactory
            .getLog(ListBlackWatchMitigationsActivity.class);

    private enum ListBlackWatchMitigationsExceptions {
        BadRequest, InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the
    // ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections
            .unmodifiableSet(Sets.newHashSet(
                    ListBlackWatchMitigationsExceptions.BadRequest.name(),
                    ListBlackWatchMitigationsExceptions.InternalError.name()));

    @NonNull
    private final RequestValidator requestValidator;
    @NonNull
    private final BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;
    public static final long MAX_NUMBER_OF_ENTRIES_TO_FETCH = 10000;

    @Validated
    @Operation("ListBlackWatchMitigations")
    @Documentation("ListBlackWatchMitigations")
    public @NonNull ListBlackWatchMitigationsResponse enact(
            @NonNull ListBlackWatchMitigationsRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(),
                "ListBlackWatchMitigations.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            LOG.info(String
                    .format("ListBlackWatchMitigationsActivity called with RequestId: %s and request: %s.",
                            requestId,
                            ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS,
                    tsdMetrics);

            // Step 1. Validate your request.
            requestValidator.validateListBlackWatchMitigationsRequest(request);
            String mitigationId = request.getMitigationId();
            String resourceId = request.getResourceId();
            String resourceType = request.getResourceType();
            String ownerARN = request.getOwnerARN();
            Long maxNumberOfEntriesToReturn = request.getMaxResults();
            if (maxNumberOfEntriesToReturn == null) {
                maxNumberOfEntriesToReturn = MAX_NUMBER_OF_ENTRIES_TO_FETCH;
            }

            // Step 2. fetch mitigations from DDB table
            // TODO: use the nextToken in the request for ddb table scan and return a new nextToken in the response
            List<BlackWatchMitigationDefinition> listOfBlackWatchMitigationDefinition = blackwatchMitigationInfoHandler
                    .getBlackWatchMitigations(mitigationId, resourceId,
                            resourceType, ownerARN, maxNumberOfEntriesToReturn,
                            tsdMetrics);
            ListBlackWatchMitigationsResponse response = new ListBlackWatchMitigationsResponse();
            response.setRequestId(requestId);
            response.setMitigationList(listOfBlackWatchMitigationDefinition);
            return response;

        } catch (IllegalArgumentException ex) {
            String msg = String.format(
                    ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT,
                    requestId, "ListBlackWatchMitigationsActivity",
                    ex.getMessage());
            LOG.warn(
                    msg + " for request: "
                            + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + ListBlackWatchMitigationsExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in ListBlackWatchMitigationsActivity for requestId: "
                    + requestId + ", reason: " + internalError.getMessage();
            LOG.error(
                    LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX
                            + msg
                            + " for request "
                            + ReflectionToStringBuilder.toString(request),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX
                    + ListBlackWatchMitigationsExceptions.InternalError.name(),
                    1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(
                    LookoutMitigationServiceConstants.ENACT_SUCCESS,
                    requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(
                    LookoutMitigationServiceConstants.ENACT_FAILURE,
                    requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}