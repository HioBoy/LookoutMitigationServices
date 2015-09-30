package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
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
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryRequest;
import com.amazon.lookout.mitigation.service.GetMitigationHistoryResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatus;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

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
    private static final Integer MAX_NUMBER_OF_HISTORY_TO_FETCH = 100;
    
    private enum GetMitigationHistoryExceptions {
        BadRequest,
        InternalError,
        MissingMitigation
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = 
            Collections.unmodifiableSet(
                    Arrays.stream(GetMitigationHistoryExceptions.values())
                    .map(e -> e.name())
                    .collect(Collectors.toSet()));
    
    private final RequestValidator requestValidator;
    private final RequestInfoHandler requestInfoHandler;
    private final MitigationInstanceInfoHandler mitigationInstanceHandler;
    
    @ConstructorProperties({"requestValidator", "requestInfoHandler", "mitigationInstanceHandler"})
    public GetMitigationHistoryActivity(@NonNull RequestValidator requestValidator, @NonNull RequestInfoHandler requestInfoHandler, 
                                     @NonNull MitigationInstanceInfoHandler mitigationInstanceHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestInfoHandler);
        this.requestInfoHandler = requestInfoHandler;
        
        Validate.notNull(mitigationInstanceHandler);
        this.mitigationInstanceHandler = mitigationInstanceHandler;
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
            
            // Step 1. Validate this request
            requestValidator.validateGetMitigationHistoryRequest(request);
            List<MitigationRequestDescriptionWithStatus> mitigationDescriptionWithStatuses = new ArrayList<>();
            
            String deviceName = request.getDeviceName();
            String deviceScope = request.getDeviceScope();
            String serviceName = request.getServiceName();
            String mitigationName = request.getMitigationName();
            Integer exclusiveStartVersion = request.getExclusiveStartVersion();
            Integer maxNumberOfHistoryEntriesToFetch = request.getMaxNumberOfHistoryEntriesToFetch();
            if (maxNumberOfHistoryEntriesToFetch == null) {
                maxNumberOfHistoryEntriesToFetch = DEFAULT_MAX_NUMBER_OF_HISTORY_TO_FETCH;
            }
            maxNumberOfHistoryEntriesToFetch = Math.min(maxNumberOfHistoryEntriesToFetch, MAX_NUMBER_OF_HISTORY_TO_FETCH);

            // Step 2. Fetch list of mitigation history
            List<MitigationRequestDescription> mitigationDescriptions = requestInfoHandler.
                    getMitigationHistoryForMitigation(serviceName, deviceName, deviceScope, mitigationName, exclusiveStartVersion, 
                            maxNumberOfHistoryEntriesToFetch, tsdMetrics);

            // Step 3. For each of the requests fetched above, query the individual instance status and 
            //     populate a new MitigationRequestDescriptionWithStatus instance to wrap this information.
            for (MitigationRequestDescription description : mitigationDescriptions) {
                List<MitigationInstanceStatus> instanceStatuses = mitigationInstanceHandler
                        .getMitigationInstanceStatus(deviceName, description.getJobId(), tsdMetrics);
                
                MitigationRequestDescriptionWithStatus mitigationDescriptionWithStatus = new MitigationRequestDescriptionWithStatus();
                mitigationDescriptionWithStatus.setMitigationRequestDescription(description);
                mitigationDescriptionWithStatus.setInstancesStatus(instanceStatuses);
                mitigationDescriptionWithStatuses.add(mitigationDescriptionWithStatus);
            }
            
            // Step 4. Create the response object to return back to the client.
            GetMitigationHistoryResponse response = new GetMitigationHistoryResponse();
            response.setDeviceName(deviceName);
            response.setDeviceScope(deviceScope);
            response.setMitigationName(mitigationName);
            response.setServiceName(serviceName);
            response.setMitigationRequestDescriptionsWithStatus(mitigationDescriptionWithStatuses);
            response.setRequestId(requestId);
            response.setExclusiveStartVersion(mitigationDescriptions.get(mitigationDescriptions.size() - 1).getMitigationVersion());
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetMitigationHistoryActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationHistoryExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (MissingMitigationException400 missingMitigationException) {
            String msg = "Caught MissingMitigationException in GetMitigationHistoryActivity for requestId: " + requestId + ", reason: " + missingMitigationException.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), missingMitigationException);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationHistoryExceptions.MissingMitigation.name(), 1);
            throw new MissingMitigationException400(msg);
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
