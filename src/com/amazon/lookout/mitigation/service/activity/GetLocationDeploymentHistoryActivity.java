package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchResourceCapacityHelper;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchResourceCapacity;
import com.amazon.coral.metrics.MetricsFactory;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
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
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.model.ArchivedRequest;
import com.amazon.lookout.mitigation.datastore.model.RequestPage;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

/**
 * Get mitigation deployment history on a specific location.
 * The returned history entries is ordered from latest to earliest.
 * The max number of history entries will be limited to 200.
 * This API queries GSI, result is eventually consistent.
 */
@Service("LookoutMitigationService")
public class GetLocationDeploymentHistoryActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetLocationDeploymentHistoryActivity.class);
    private static final Integer DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH = 20;
    public static final Integer MAX_NUMBER_OF_HISTORY_TO_FETCH = 200;
    
    private enum GetLocationDeploymentHistoryExceptions {
        BadRequest,
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetLocationDeploymentHistoryExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet())); 
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final RequestsDAO requestsDao;


    @NonNull
    private final MetricsFactory metricsFactory;

    @ConstructorProperties({"requestValidator", "requestsDao"})
    public GetLocationDeploymentHistoryActivity(@NonNull RequestValidator requestValidator,
            @NonNull final RequestsDAO requestsDao) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
        this.metricsFactory = null;
    }

    public GetLocationDeploymentHistoryActivity(@NonNull RequestValidator requestValidator,
                                                @NonNull final RequestsDAO requestsDao,
                                                @NonNull MetricsFactory metricsFactory) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
        this.metricsFactory = metricsFactory;
    }

    // Turn a CurrentRequest into a LocationDeploymentInfo
    private static LocationDeploymentInfo getLocationDeploymentInfo(
            @NonNull final CurrentRequest currentRequest) {
        final LocationDeploymentInfo info = new LocationDeploymentInfo();
        info.setMitigationStatus(currentRequest.getStatus());
        info.setSchedulingStatus(currentRequest.getWorkflowStatus());
        info.setDeploymentHistory(currentRequest.getDeploymentHistory());
        info.setDeployDate(currentRequest.getCreateDate());
        info.setMitigationName(currentRequest.getMitigationName());
        info.setMitigationVersion(currentRequest.getMitigationVersion());
        info.setJobId(currentRequest.getWorkflowId());
        return info;
    }

    @Validated
    @Operation("GetLocationDeploymentHistory")
    @Documentation("GetLocationDeploymentHistory")
    public @NonNull GetLocationDeploymentHistoryResponse enact(@NonNull GetLocationDeploymentHistoryRequest request) {


        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetLocationDeploymentHistory.enact");
        try {            
            LOG.info(String.format("GetLocationDeploymentHistoryActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateGetLocationDeploymentHistoryRequest(request);
            
            final DeviceName device = DeviceName.valueOf(request.getDeviceName());
            String location = request.getLocation();
            
            Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
            if (maxNumberOfHistoryEntriesToFetch == null) {
                maxNumberOfHistoryEntriesToFetch = DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH;
            }
            maxNumberOfHistoryEntriesToFetch = Math.min(maxNumberOfHistoryEntriesToFetch, MAX_NUMBER_OF_HISTORY_TO_FETCH);

            // Get requests
            final RequestPage<CurrentRequest> page = requestsDao.getLocationRequestsPage(
                    device, location, maxNumberOfHistoryEntriesToFetch,
                    request.getExclusiveLastEvaluatedKey());

            String lastEvaluatedKey = page.getLastEvaluatedKey();

            final List<LocationDeploymentInfo> listOfLocationDeploymentInfo = new ArrayList<>();

            for (CurrentRequest currentRequest : page.getPage()) {
                listOfLocationDeploymentInfo.add(getLocationDeploymentInfo(currentRequest));
            }

            // Create the response object to return to the client.
            GetLocationDeploymentHistoryResponse response = new GetLocationDeploymentHistoryResponse();
            response.setDeviceName(device.name());
            response.setLocation(location);
            response.setListOfLocationDeploymentInfo(listOfLocationDeploymentInfo);
            response.setRequestId(requestId);
            response.setExclusiveLastEvaluatedKey(lastEvaluatedKey);
            
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetLocationDeploymentHistoryActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetLocationDeploymentHistoryExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in GetLocationDeploymentHistoryActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetLocationDeploymentHistoryExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

