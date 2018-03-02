package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

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
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

@ThreadSafe
@Service("LookoutMitigationService")
public class GetRequestStatusActivity extends Activity{
    private static final Log LOG = LogFactory.getLog(GetRequestStatusActivity.class);
    
    private enum GetRequestStatusExceptions {
        BadRequest,
        InternalError
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(GetRequestStatusExceptions.BadRequest.name(),
                                                                                                      GetRequestStatusExceptions.InternalError.name()));
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final RequestsDAO requestsDao;
    
    @ConstructorProperties({"requestValidator", "requestsDao"})
    public GetRequestStatusActivity(@NonNull RequestValidator requestValidator,
            @NonNull final RequestsDAO requestsDao) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
    }
    
    @Validated
    @Operation("GetRequestStatus")
    @Documentation("GetRequestStatus")
    public @NonNull GetRequestStatusResponse enact(@NonNull GetRequestStatusRequest request) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetRequestStatus.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            final long jobId = request.getJobId();
            final String location = request.getLocation();
            final DeviceName device = DeviceName.valueOf(request.getDeviceName());

            LOG.info(String.format("GetRequestStatusActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateGetRequestStatusRequest(request);

            // Get request
            final CurrentRequest currentRequest = requestsDao.retrieveRequest(
                    device, location, jobId);

            if (currentRequest == null) {
                // Do whatever crazy thing old mitigation service does in this case
                final String msg = String.format("Unable to find request for "
                        + "DeviceName %s, location %s, jobId %d", device.name(),
                        location, jobId);
                throw new IllegalStateException(msg);
            }

            final String mitigationName = currentRequest.getMitigationName();
            final String workflowStatus = currentRequest.getWorkflowStatus();

            // There is only one instance
            // TODO: this should no longer return a list
            final MitigationInstanceStatus instance = new MitigationInstanceStatus();
            instance.setLocation(currentRequest.getLocation());
            instance.setMitigationStatus(currentRequest.getStatus());
            instance.setDeploymentHistory(currentRequest.getDeploymentHistory());
            instance.setDeployDate(currentRequest.getCreateDateMillis());
            final List<MitigationInstanceStatus> mitigationStatusesResult = new ArrayList<>();
            mitigationStatusesResult.add(instance);

            GetRequestStatusResponse response = new GetRequestStatusResponse();
            response.setMitigationName(mitigationName);
            response.setDeviceName(device.name());
            response.setRequestStatus(workflowStatus);
            response.setMitigationInstanceStatuses(mitigationStatusesResult);
            
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetRequestStatusActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetRequestStatusExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in GetRequestStatusActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetRequestStatusExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

