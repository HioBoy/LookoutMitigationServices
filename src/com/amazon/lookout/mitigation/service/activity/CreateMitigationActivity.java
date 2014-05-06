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
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(CreateMitigationActivity.class);
    
    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter workflowStarter;
    
    public CreateMitigationActivity(@Nonnull RequestValidator requestValidator, @Nonnull TemplateBasedRequestValidator templateBasedValidator, 
                                    @Nonnull RequestStorageManager requestStorageManager, @Nonnull SWFWorkflowStarter workflowStarter) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(templateBasedValidator);
        this.templateBasedValidator = templateBasedValidator;
        
        Validate.notNull(requestStorageManager);
        this.requestStorageManager = requestStorageManager;
        
        Validate.notNull(workflowStarter);
        this.workflowStarter = workflowStarter;
    }

    @Validated
    @Operation("CreateMitigation")
    @Documentation("CreateMitigation")
    public @Nonnull MitigationModificationResponse enact(@Nonnull MitigationModificationRequest createMitigationRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics());
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("CreateMitigationActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(createMitigationRequest)));
            
            // Step1. Authorize this request.
            // TODO - Once we have the auth model finalized, we should perform authorization checks here.
            // LookoutMitigationServiceAuthorizer authorizer; (dependency injected in the constructor)
            // boolean isAuthorized = authorizer.authorizeRequest(createMitigationRequest.getServiceName(), createMitigationRequest.getMitigationTemplate(), createMitigationRequest.getMitigationActionMetadata());
            // if (!isAuthorized) {
            //     MitigationActionMetadata metadata = createMitigationRequest.getMitigationActionMetadata();
            //     throw new IllegalArgumentException(metadata.getUser() + " not authorized to CreateMitigation for service: " + createMitigationRequest.getServiceName() + 
            //                                        " using mitigation template: " + createMitigationRequest.getMitigationTemplate());
            // }
            
            // Step2. Validate this request.
            requestValidator.validateCreateRequest(createMitigationRequest);
            
            // Step3. Validate this request based on the template.
            templateBasedValidator.validateCreateRequestForTemplate(createMitigationRequest, tsdMetrics);
            
            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(createMitigationRequest, RequestType.CreateRequest, tsdMetrics);
            
            // Step5. Create new workflow client to be used for running the workflow.
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(createMitigationRequest.getMitigationTemplate());
            String deviceName = deviceNameAndScope.getDeviceName().name();
            WorkflowClientExternal workflowClient = workflowStarter.createSWFWorkflowClient(workflowId, createMitigationRequest, deviceName, tsdMetrics);
            
            // Step6. Update the record for this workflow request and store the runId that SWF associates with this workflow.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.CreateRequest, tsdMetrics);
            
            // Step7. Now that the request has been updated with the swfRunId associated with this SWF workflow run, start running the workflow.
            workflowStarter.startWorkflow(workflowId, createMitigationRequest, RequestType.CreateRequest, 
                                          DDBBasedCreateRequestStorageHandler.INITIAL_MITIGATION_VERSION, deviceName, workflowClient, tsdMetrics);
            
            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(createMitigationRequest.getMitigationName());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(createMitigationRequest.getServiceName());
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.SCHEDULED);
            
            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in CreateMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createMitigationRequest));
            LOG.warn(msg, ex);
            throw new BadRequest400(msg, ex);
        } catch (DuplicateDefinitionException400 ex) {
            String msg = String.format("Caught DuplicateDefinitionException in CreateMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createMitigationRequest));
            LOG.warn(msg, ex);
            throw ex;
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for CreateMitigationActivity: for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createMitigationRequest));
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
