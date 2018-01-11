package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Collections;
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
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.AbortDeploymentRequest;
import com.amazon.lookout.mitigation.service.AbortDeploymentResponse;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.google.common.collect.Sets;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.CurrentRequestsDAO;

@ThreadSafe
@Service("LookoutMitigationService")
public class AbortDeploymentActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(AbortDeploymentActivity.class);
    private static final String WORKFLOWCOMPLETED_ABORTFAIL_STATUS = "WORKFLOWCOMPLETED_ABORTFAILED";
    
    private enum AbortDeploymentExceptions {
        BadRequest,
        InternalError
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(AbortDeploymentExceptions.BadRequest.name(),
    		AbortDeploymentExceptions.InternalError.name()));
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final CurrentRequestsDAO currentDao;
    
    @ConstructorProperties({"requestValidator", "currentDao"})
    public AbortDeploymentActivity(@NonNull RequestValidator requestValidator,
            @NonNull final CurrentRequestsDAO currentDao) {
        this.requestValidator = requestValidator;
        this.currentDao = currentDao;
    }

    @Validated
    @Operation("AbortDeployment")
    @Documentation("AbortDeployment")
    public @NonNull AbortDeploymentResponse enact(@NonNull AbortDeploymentRequest abortRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "AbortDeployment.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            long jobId = abortRequest.getJobId();
            final String location = abortRequest.getLocation();
            final DeviceName device = DeviceName.valueOf(abortRequest.getDeviceName());

            LOG.info(String.format("AbortDeploymentActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(abortRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateAbortDeploymentRequest(abortRequest);

            String requestStatus = null;
            String mitigationName = null;

            // Find the request in the current requests table
            final CurrentRequest currentRequest = currentDao.retrieveRequest(
                    device, location, jobId);

            if (currentRequest != null) {
                requestStatus = currentRequest.getWorkflowStatus();
                mitigationName = currentRequest.getMitigationName();

                // Set the abort flag
                currentRequest.setAborted(true);
                currentDao.updateRequest(currentRequest);
            } else {
                // If it isn't in the current table, then it is either already
                // finished or has never existed.
                requestStatus = WORKFLOWCOMPLETED_ABORTFAIL_STATUS;
            }

            AbortDeploymentResponse response = new AbortDeploymentResponse();
            response.setMitigationName(mitigationName);
            response.setDeviceName(device.name());
            response.setRequestStatus(requestStatus);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "AbortDeploymentActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(abortRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + AbortDeploymentExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in AbortDeploymentActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(abortRequest),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + AbortDeploymentExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

