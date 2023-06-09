package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazon.lookout.mitigation.datastore.model.GetMitigationHistoryResult;
import com.amazon.lookout.mitigation.service.FailedMitigationDeployments;
import lombok.NonNull;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

/**
 * Get mitigation history by mitigation name and device name.
 * The returned history entries is ordered from latest to earliest.
 * The max number of history entries will be limited to 100.
 * This API queries GSI, result is eventually consistent.
 */
@Service("LookoutMitigationService")
public class GetMitigationHistoryActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetMitigationHistoryActivity.class);
    private static final Integer DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH = 20;
    public static final Integer MAX_NUMBER_OF_HISTORY_TO_FETCH = 100;
    
    private enum GetMitigationHistoryExceptions {
        BadRequest,
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = 
            Collections.unmodifiableSet(
                    Arrays.stream(GetMitigationHistoryExceptions.values())
                    .map(e -> e.name())
                    .collect(Collectors.toSet()));
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final RequestsDAO requestsDao;
    
    @ConstructorProperties({"requestValidator", "requestsDao"})
    public GetMitigationHistoryActivity(@NonNull RequestValidator requestValidator,
            @NonNull final RequestsDAO requestsDao) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
    }

    @Validated
    @Operation("GetMitigationHistory")
    @Documentation("GetMitigationHistory")
    public @NonNull GetMitigationHistoryResponse enact(@NonNull GetMitigationHistoryRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetMitigationHistory.enact");
        try {            
            LOG.info(String.format("GetMitigationHistoryActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateGetMitigationHistoryRequest(request);

            Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
            if (maxNumberOfHistoryEntriesToFetch == null) {
                maxNumberOfHistoryEntriesToFetch = DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH;
            }
            maxNumberOfHistoryEntriesToFetch = Math.min(maxNumberOfHistoryEntriesToFetch, MAX_NUMBER_OF_HISTORY_TO_FETCH);

            final DeviceName device = DeviceName.valueOf(request.getDeviceName());
            final String location = request.getLocation();
            final String mitigationName = request.getMitigationName();
            final String lastEvaluatedKey = request.getExclusiveLastEvaluatedKey();

            // Get the mitigation history
            final GetMitigationHistoryResult result
                = requestsDao.getMitigationHistory(device, location, mitigationName,
                        maxNumberOfHistoryEntriesToFetch, lastEvaluatedKey);

            // Create the response object to return to the client.
            GetMitigationHistoryResponse response = new GetMitigationHistoryResponse();
            response.setDeviceName(device.name());
            response.setMitigationName(mitigationName);
            response.setListOfMitigationRequestDescriptionsWithLocationAndStatus(result.getRequestPage().getPage());
            response.setRequestId(requestId);
            response.setExclusiveLastEvaluatedKey(result.getRequestPage().getLastEvaluatedKey());
            response.setFailedMitigationDeployments(FailedMitigationDeployments.builder()
                    .withLastFailedJob(result.getMostRecentFailedJob())
                    .withNumRecentFailures(result.getNumRecentFailures())
                    .build());
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetMitigationHistoryActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationHistoryExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in GetMitigationHistoryActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationHistoryExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

