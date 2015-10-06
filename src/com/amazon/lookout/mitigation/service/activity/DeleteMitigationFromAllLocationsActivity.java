package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
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
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;
import com.google.common.collect.Sets;

@ThreadSafe
@Service("LookoutMitigationService")
public class DeleteMitigationFromAllLocationsActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(DeleteMitigationFromAllLocationsActivity.class);
    
    private enum DeleteExceptions {
        BadRequest,
        DuplicateRequest,
        MissingMitigation,
        InternalError
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(DeleteExceptions.BadRequest.name(),
                                                                                                      DeleteExceptions.DuplicateRequest.name(),
                                                                                                      DeleteExceptions.MissingMitigation.name(),
                                                                                                      DeleteExceptions.InternalError.name())); 
    
    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter workflowStarter;
    private final TemplateBasedLocationsManager templateBasedLocationsManager;

    @ConstructorProperties({"requestValidator", "templateBasedValidator", "requestStorageManager", "swfWorkflowStarter",
            "templateBasedLocationsManager"})
    public DeleteMitigationFromAllLocationsActivity(@NonNull RequestValidator requestValidator,
                                                    @NonNull TemplateBasedRequestValidator templateBasedValidator,
                                                    @NonNull RequestStorageManager requestStorageManager,
                                                    @NonNull SWFWorkflowStarter workflowStarter,
                                                    @NonNull TemplateBasedLocationsManager templateBasedLocationsManager) {
        this.requestValidator = requestValidator;
        this.templateBasedValidator = templateBasedValidator;
        this.requestStorageManager = requestStorageManager;
        this.workflowStarter = workflowStarter;
        this.templateBasedLocationsManager = templateBasedLocationsManager;
    }

    @Validated
    @Operation("DeleteMitigationFromAllLocations")
    @Documentation("DeleteMitigationFromAllLocations")
    public @NonNull MitigationModificationResponse enact(@NonNull DeleteMitigationFromAllLocationsRequest deleteRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "DeleteMitigationFromAllLocations.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("DeleteMitigationFromAllLocations called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(deleteRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            String mitigationTemplate = deleteRequest.getMitigationTemplate();
            ActivityHelper.addTemplateNameCountMetrics(mitigationTemplate, tsdMetrics);
            
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            String deviceName = deviceNameAndScope.getDeviceName().name();
            ActivityHelper.addDeviceNameCountMetrics(deviceName, tsdMetrics);
            
            String deviceScope = deviceNameAndScope.getDeviceScope().name();
            ActivityHelper.addDeviceScopeCountMetrics(deviceScope, tsdMetrics);
            
            String serviceName = deleteRequest.getServiceName();
            ActivityHelper.addServiceNameCountMetrics(serviceName, tsdMetrics);

            // Step1. Validate this request.
            requestValidator.validateDeleteRequest(deleteRequest);
            
            // Step2. Validate this request based on the template.
            templateBasedValidator.validateRequestForTemplate(deleteRequest, tsdMetrics);

            // Step3. Get the locations where we need to start running the workflow.
            // In most cases it is provided by the client, but for some templates we might have locations based on the templateName, 
            // hence checking with the templateBasedLocationsHelper and also passing it the original request to have the entire context.
            Set<String> locationsToDeploy = templateBasedLocationsManager.getLocationsForDeployment(deleteRequest);
            
            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(deleteRequest, locationsToDeploy, RequestType.DeleteRequest, tsdMetrics);
            
            // Step5. Create new workflow client to be used for running the workflow.
            WorkflowClientExternal workflowClient = workflowStarter.createMitigationModificationWorkflowClient(workflowId, deleteRequest, deviceName, tsdMetrics);
            
            // Step6. Start running the workflow.
            workflowStarter.startMitigationModificationWorkflow(workflowId, deleteRequest, locationsToDeploy, RequestType.DeleteRequest, 
                                          deleteRequest.getMitigationVersion(), deviceName, deviceScope, workflowClient, tsdMetrics);
            
            // Step7. Once the workflow is running, it should have an associated swfRunId, update the request record for this workflow request and store the associated runId.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.DeleteRequest, tsdMetrics);
            
            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(deleteRequest.getMitigationName());
            mitigationModificationResponse.setMitigationTemplate(deleteRequest.getMitigationTemplate());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(deleteRequest.getServiceName());
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
        } catch (IllegalArgumentException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "DeleteMitigationFromAllLocationsActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(deleteRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (DuplicateRequestException400 ex) {
            String msg = "Caught DuplicateDefinitionException in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(deleteRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.DuplicateRequest.name(), 1);
            throw new DuplicateRequestException400(msg);
        } catch (MissingMitigationException400 ex) {
            String msg = "Caught MissingMitigationException in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(deleteRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.MissingMitigation.name(), 1);
            throw new MissingMitigationException400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in DeleteMitigationActivity: for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(deleteRequest),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            if (requestSuccessfullyProcessed) {
                tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, 1);
                tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 0);
            } else {
                tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, 0);
                tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 1);
            }
            tsdMetrics.end();
        }
    } 
}
