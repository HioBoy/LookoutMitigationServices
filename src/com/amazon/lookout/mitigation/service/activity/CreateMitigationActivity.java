package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;

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
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedCreateRequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.helpers.AWSUserGroupBasedAuthorizer;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(CreateMitigationActivity.class);
    
    private static final String OPERATION_NAME_FOR_AUTH_CHECK = "createMitigation";
    
    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter workflowStarter;
    private final AWSUserGroupBasedAuthorizer authorizer;
    
    @ConstructorProperties({"requestValidator", "templateBasedValidator", "requestStorageManager", "swfWorkflowStarter", "authorizer"})
    public CreateMitigationActivity(@Nonnull RequestValidator requestValidator, @Nonnull TemplateBasedRequestValidator templateBasedValidator, 
                                    @Nonnull RequestStorageManager requestStorageManager, @Nonnull SWFWorkflowStarter workflowStarter, @Nonnull AWSUserGroupBasedAuthorizer authorizer) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(templateBasedValidator);
        this.templateBasedValidator = templateBasedValidator;
        
        Validate.notNull(requestStorageManager);
        this.requestStorageManager = requestStorageManager;
        
        Validate.notNull(workflowStarter);
        this.workflowStarter = workflowStarter;
        
        Validate.notNull(authorizer);
        this.authorizer = authorizer;
    }

    @Validated
    @Operation("CreateMitigation")
    @Documentation("CreateMitigation")
    public @Nonnull MitigationModificationResponse enact(@Nonnull CreateMitigationRequest createRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics());
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("CreateMitigationActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(createRequest)));
            
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(createRequest.getMitigationTemplate());
            String deviceName = deviceNameAndScope.getDeviceName().name();
            String serviceName = createRequest.getServiceName();
            
            // Step1. Authorize this request. TODO - Turn on this auth check when the auth issues are all fixed.
            /*boolean isAuthorized = authorizer.isClientAuthorized(getIdentity(), serviceName, deviceName, OPERATION_NAME_FOR_AUTH_CHECK);
            if (!isAuthorized) {
                authorizer.setAuthorizedFlag(getIdentity(), false);
                
                MitigationActionMetadata metadata = createRequest.getMitigationActionMetadata();
                String msg = metadata.getUser() + " not authorized to call CreateMitigation for service: " + createRequest.getServiceName() + 
                             " for device: " + deviceName + " using mitigation template: " + createRequest.getMitigationTemplate() + ". Request signed with AccessKeyId: " + 
                             getIdentity().getAttribute(Identity.AWS_ACCESS_KEY) + " and belonging to groups: " + getIdentity().getAttribute(Identity.AWS_USER_GROUPS);
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            } else {
                authorizer.setAuthorizedFlag(getIdentity(), true);
            }*/
            
            // Step2. Validate this request.
            requestValidator.validateCreateRequest(createRequest);
            
            // Step3. Validate this request based on the template.
            templateBasedValidator.validateCreateRequestForTemplate(createRequest, tsdMetrics);
            
            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(createRequest, RequestType.CreateRequest, tsdMetrics);
            
            // Step5. Create new workflow client to be used for running the workflow.
            
            WorkflowClientExternal workflowClient = workflowStarter.createSWFWorkflowClient(workflowId, createRequest, deviceName, tsdMetrics);
            
            // Step6. Start running the workflow.
            workflowStarter.startWorkflow(workflowId, createRequest, RequestType.CreateRequest, 
                                          DDBBasedCreateRequestStorageHandler.INITIAL_MITIGATION_VERSION, deviceName, workflowClient, tsdMetrics);
            
            // Step7. Update the record for this workflow request and store the runId that SWF associates with this workflow.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.CreateRequest, tsdMetrics);
            
            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(createRequest.getMitigationName());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(serviceName);
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.RUNNING);
            
            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in CreateMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createRequest));
            LOG.warn(msg, ex);
            throw new BadRequest400(msg, ex);
        } catch (DuplicateDefinitionException400 ex) {
            String msg = String.format("Caught DuplicateDefinitionException in CreateMitigationActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createRequest));
            LOG.warn(msg, ex);
            throw ex;
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for CreateMitigationActivity: for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(createRequest));
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
