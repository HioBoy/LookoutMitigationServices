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
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
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
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

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
    
    private final RequestStorageManager requestStorageManager;
    private final RequestInfoHandler requestInfoHandler;
    private final RequestValidator requestValidator;
    @NonNull private final CurrentRequestsDAO currentDao;
    @NonNull private final SwitcherooDAO switcherooDao;
    
    @ConstructorProperties({"requestValidator", "requestInfoHandler", "requestStorageManager",
    "currentDao", "switcherooDao"})
    public AbortDeploymentActivity(@NonNull RequestValidator requestValidator,
            @NonNull RequestInfoHandler requestInfoHandler,
            @NonNull RequestStorageManager requestStorageManager,
            @NonNull final CurrentRequestsDAO currentDao,
            @NonNull final SwitcherooDAO switcherooDao) {
        this.requestValidator = requestValidator;
        this.requestInfoHandler = requestInfoHandler;
        this.requestStorageManager = requestStorageManager;
        this.currentDao = currentDao;
        this.switcherooDao = switcherooDao;
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
            String deviceName = abortRequest.getDeviceName();
            String templateName = abortRequest.getMitigationTemplate();
            String serviceName = abortRequest.getServiceName();
            long jobId = abortRequest.getJobId();
            final String location = abortRequest.getLocation();

            // A real typed device
            final DeviceName device = DeviceName.valueOf(deviceName);

            LOG.info(String.format("AbortDeploymentActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(abortRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Step 1. Validate this request
            requestValidator.validateAbortDeploymentRequest(abortRequest);

            String requestStatus = null;
            String mitigationName = null;

            if (switcherooDao.useNewMitigationService(device, location)) {
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
            } else {
                // Step 2. locate the request in DDB and get the current status
                MitigationNameAndRequestStatus mitigationNameAndRequestStatus = requestInfoHandler.getMitigationNameAndRequestStatus(deviceName, templateName, jobId, tsdMetrics);
                requestStatus = mitigationNameAndRequestStatus.getRequestStatus();
                mitigationName = mitigationNameAndRequestStatus.getMitigationName();

                // Step 3. take action based on the current workflow status of the request.
                if (requestStatus.equals(WorkflowStatus.RUNNING) ) {
                    //If the workflow is still running, set the abort flag of the request in DDB
                    requestStorageManager.requestAbortForWorkflowRequest(deviceName, jobId, tsdMetrics);
                } else {
                    LOG.info(String.format("Abort failed, current request status: %s.", requestStatus));
                    //otherwise, it is too late to abort, return abort failed
                    requestStatus = WORKFLOWCOMPLETED_ABORTFAIL_STATUS;
                }
            }

            AbortDeploymentResponse response = new AbortDeploymentResponse();
            response.setMitigationName(mitigationName);
            response.setServiceName(serviceName);
            response.setDeviceName(deviceName);
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
