package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.amazon.lookout.ddb.model.MitigationInstancesModel;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryRequest;
import com.amazon.lookout.mitigation.service.GetLocationDeploymentHistoryResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.LocationDeploymentInfo;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

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
    private static final Integer MAX_NUMBER_OF_HISTORY_TO_FETCH = 200;
    
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
    
    private final RequestValidator requestValidator;
    private final MitigationInstanceInfoHandler mitigationInstanceHandler;
    
    @ConstructorProperties({"requestValidator", "mitigationInstanceHandler"})
    public GetLocationDeploymentHistoryActivity(@NonNull RequestValidator requestValidator,
                                     @NonNull MitigationInstanceInfoHandler mitigationInstanceHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(mitigationInstanceHandler);
        this.mitigationInstanceHandler = mitigationInstanceHandler;
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
            
            // Step 1. Validate this request
            requestValidator.validateGetLocationDeploymentHistoryRequest(request);
            
            String deviceName = request.getDeviceName();
            String location = request.getLocation();
            String serviceName = request.getServiceName();
            
            Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
            if (maxNumberOfHistoryEntriesToFetch == null) {
                maxNumberOfHistoryEntriesToFetch = DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH;
            }
            maxNumberOfHistoryEntriesToFetch = Math.min(maxNumberOfHistoryEntriesToFetch, MAX_NUMBER_OF_HISTORY_TO_FETCH);

            // Step 2. Fetch list of mitigation deployment history on this location
            List<LocationDeploymentInfo> listOfLocationDeploymentInfo = 
                    mitigationInstanceHandler.getLocationDeploymentInfoOnLocation(deviceName, serviceName, location,
                            maxNumberOfHistoryEntriesToFetch, request.getExclusiveLastEvaluatedTimestamp(), tsdMetrics);
            
            // Step 3. Create the response object to return back to the client.
            GetLocationDeploymentHistoryResponse response = new GetLocationDeploymentHistoryResponse();
            response.setDeviceName(deviceName);
            response.setLocation(location);
            response.setListOfLocationDeploymentInfo(listOfLocationDeploymentInfo);
            response.setRequestId(requestId);
            if (!listOfLocationDeploymentInfo.isEmpty()) {
                String deployDateUTC = listOfLocationDeploymentInfo.get(listOfLocationDeploymentInfo.size() - 1).getDeployDate();
                response.setExclusiveLastEvaluatedTimestamp(
                        MitigationInstancesModel.CREATE_DATE_FORMATTER.parseMillis(deployDateUTC));
            }
            
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
