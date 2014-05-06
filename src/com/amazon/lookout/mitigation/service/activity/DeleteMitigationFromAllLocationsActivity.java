package com.amazon.lookout.mitigation.service.activity;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;
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
import com.amazon.lookout.mitigation.service.DeleteMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

@ThreadSafe
@Service("LookoutMitigationService")
public class DeleteMitigationFromAllLocationsActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(DeleteMitigationFromAllLocationsActivity.class);
    
    private final RequestValidator requestValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter workflowStarter;
    
    public DeleteMitigationFromAllLocationsActivity(@Nonnull RequestValidator requestValidator, @Nonnull RequestStorageManager requestStorageManager, @Nonnull SWFWorkflowStarter workflowStarter) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestStorageManager);
        this.requestStorageManager = requestStorageManager;
        
        Validate.notNull(workflowStarter);
        this.workflowStarter = workflowStarter;
    }

    @Validated
    @Operation("DeleteMitigationFromAllLocations")
    @Documentation("DeleteMitigationFromAllLocations")
    public @Nonnull MitigationModificationResponse enact(@Nonnull DeleteMitigationRequest deleteRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics());
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("DeleteMitigationFromAllLocations called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(deleteRequest)));
            
            // Step1. Authorize this request.
            // TODO - Once we have the auth changes finalized, we should perform authorization checks here.
            // LookoutMitigationServiceAuthorizer authorizer; (dependency injected in the constructor)
            // boolean isAuthorized = authorizer.authorizeRequest(deleteRequest.getServiceName(), deleteRequest.getMitigationTemplate(), deleteRequest.getMitigationActionMetadata());
            // if (!isAuthorized) {
            //     MitigationActionMetadata metadata = deleteRequest.getMitigationActionMetadata();
            //     throw new IllegalArgumentException(metadata.getUser() + " not authorized to DeleteMitigation for service: " + deleteRequest.getServiceName() + 
            //                                        " using mitigation template: " + deleteRequest.getMitigationTemplate());
            // }
            
            // Step2. Validate this request.
            requestValidator.validateDeleteRequest(deleteRequest);
            
            // Step3. Validate this request based on the template.
            // No-op for now. There isn't anything we check based on the template for the delete requests, hence not invoking the templateBasedValidator here. 
            // If we ever do have an use-case for it, this would be the place to invoke the template-based request instance validator.
            
            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(deleteRequest, RequestType.DeleteRequest, tsdMetrics);
            
            // Step5. Create new workflow client to be used for running the workflow.
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(deleteRequest.getMitigationTemplate());
            String deviceName = deviceNameAndScope.getDeviceName().name();
            WorkflowClientExternal workflowClient = workflowStarter.createSWFWorkflowClient(workflowId, deleteRequest, deviceName, tsdMetrics);
            
            // Step6. Update the record for this workflow request and store the runId that SWF associates with this workflow.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.DeleteRequest, tsdMetrics);
            
            // Step7. Now that the request has been updated with the swfRunId associated with this SWF workflow run, start running the workflow.
            workflowStarter.startWorkflow(workflowId, deleteRequest, RequestType.DeleteRequest, deleteRequest.getMitigationVersion(), deviceName, workflowClient, tsdMetrics);
            
            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(deleteRequest.getMitigationName());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(deleteRequest.getServiceName());
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.SCHEDULED);
            
            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in DeleteMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.warn(msg, ex);
            throw new BadRequest400(msg, ex);
        } catch (DuplicateDefinitionException400 ex) {
            String msg = String.format("Caught DuplicateDefinitionException in DeleteMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.warn(msg, ex);
            throw ex;
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for DeleteMitigationActivity: for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.error(msg, internalError);
            requestSuccessfullyProcessed = false;
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
