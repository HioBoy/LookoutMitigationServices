package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;

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
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.request.RollbackMitigationRequestInternal;
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

@ThreadSafe
@Service("LookoutMitigationService")
public class RollbackMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(RollbackMitigationActivity.class);

    private enum RollbackMitigationExceptions {
        BadRequest,
        InternalError,
        MissingMitigationVersion,
        StaleRequest,
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(RollbackMitigationExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));  

    private final RequestValidator requestValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter swfWorkflowStarter;
    private final RequestInfoHandler requestInfoHandler;
    private final TemplateBasedRequestValidator templateBasedValidator;
    
    @ConstructorProperties({"requestValidator", "requestStorageManager", "swfWorkflowStarter",
        "requestInfoHandler", "templateBasedValidator"})
    public RollbackMitigationActivity(@NonNull RequestValidator requestValidator,
            @NonNull RequestStorageManager requestStorageManager, @NonNull SWFWorkflowStarter swfWorkflowStarter,
            @NonNull RequestInfoHandler requestInfoHandler, @NonNull TemplateBasedRequestValidator templateBasedValidator) {

        this.requestValidator = requestValidator;
        this.requestStorageManager = requestStorageManager;
        this.swfWorkflowStarter = swfWorkflowStarter;
        this.requestInfoHandler = requestInfoHandler;
        this.templateBasedValidator = templateBasedValidator;
    }

    @Validated
    @Operation("RollbackMitigation")
    @Documentation("RollbackMitigation")
    public @NonNull MitigationModificationResponse enact(@NonNull RollbackMitigationRequest rollbackRequest) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "RollbackMitigation.enact");
        try {
            LOG.debug(String.format("RollbackMitigationActivity called with RequestId: %s and Request: %s.",
                    requestId, ReflectionToStringBuilder.toString(rollbackRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step1. Validate this request.
            requestValidator.validateRollbackRequest(rollbackRequest);
            templateBasedValidator.validateRequestForTemplate(rollbackRequest, tsdMetrics); 
            
            // Step2. Get original mitigation modification request 
            String mitigationName = rollbackRequest.getMitigationName();
            String serviceName = rollbackRequest.getServiceName();
            int rollbackToMitigationVersion = rollbackRequest.getRollbackToMitigationVersion();
            
            String mitigationTemplate = rollbackRequest.getMitigationTemplate();
            String deviceName = MitigationTemplateToDeviceMapper.getDeviceNameForTemplate(mitigationTemplate).name();
            ActivityHelper.addDeviceNameCountMetrics(deviceName, tsdMetrics);

            MitigationRequestDescriptionWithLocations originalModificationRequest = requestInfoHandler.getMitigationDefinition(
                    deviceName, serviceName, mitigationName, rollbackToMitigationVersion, tsdMetrics);
            
            // Step3. add the found mitigation definition to new mitigation rollback request
            Set<String> locationsToDeploy = new HashSet<>(originalModificationRequest.getLocations());
            MitigationRequestDescription mitigationRequestDescription = originalModificationRequest.getMitigationRequestDescription();
            // validate the request against the record stored in system
            requestValidator.validateRollbackRequest(rollbackRequest, mitigationRequestDescription);
            
            RollbackMitigationRequestInternal internalRequest = new RollbackMitigationRequestInternal(
                    rollbackRequest, mitigationRequestDescription.getMitigationDefinition());
            
            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            RequestStorageResponse requestStorageResponse = requestStorageManager.storeRequestForWorkflow(internalRequest, locationsToDeploy, 
                    RequestType.RollbackRequest, tsdMetrics);
            
            long workflowId = requestStorageResponse.getWorkflowId();
            int storedMitigationVersion = requestStorageResponse.getMitigationVersion();

            // Step5. Create new workflow client to be used for running the workflow.
            WorkflowClientExternal workflowClient = swfWorkflowStarter.createMitigationModificationWorkflowClient(
                    workflowId, internalRequest, deviceName, tsdMetrics);

            // Step6. Start running the workflow.
            swfWorkflowStarter.startMitigationModificationWorkflow(workflowId, internalRequest, locationsToDeploy,
                    RequestType.RollbackRequest, storedMitigationVersion, deviceName,
                    workflowClient, tsdMetrics);

            // Step7. Update the record for this workflow request and store the runId that SWF associates with this workflow.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.RollbackRequest, tsdMetrics);

            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(mitigationName);
            mitigationModificationResponse.setMitigationVersion(storedMitigationVersion);
            mitigationModificationResponse.setMitigationTemplate(mitigationTemplate);
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(serviceName);
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.RUNNING);

            List<MitigationInstanceStatus> instanceStatuses = new ArrayList<>();
            for (String location : locationsToDeploy) {
                MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
                instanceStatus.setLocation(location);
                instanceStatus.setMitigationStatus(MitigationStatus.CREATED);
                instanceStatuses.add(instanceStatus);
            }
            mitigationModificationResponse.setMitigationInstanceStatuses(instanceStatuses);

            return mitigationModificationResponse;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "RollbackMitigationActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(rollbackRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RollbackMitigationExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (StaleRequestException400 ex) {
            LOG.warn(String.format("Caught " + ex.getClass().getSimpleName() + " in for StaleRequestException400 requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(rollbackRequest)), ex);
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RollbackMitigationExceptions.StaleRequest.name());
            throw ex;
        } catch (MissingMitigationVersionException404 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in RollbackMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(rollbackRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RollbackMitigationExceptions.MissingMitigationVersion.name(), 1);
            throw new MissingMitigationVersionException404(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in RollbackMitigationActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(rollbackRequest), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RollbackMitigationExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    } 
}

