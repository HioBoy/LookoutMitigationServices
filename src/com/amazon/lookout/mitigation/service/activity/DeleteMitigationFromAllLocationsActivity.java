package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateRequestException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
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
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(DeleteExceptions.BadRequest.name(),
                                                                                                      DeleteExceptions.DuplicateRequest.name(),
                                                                                                      DeleteExceptions.MissingMitigation.name(),
                                                                                                      DeleteExceptions.InternalError.name())); 
    
    private final RequestValidator requestValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter workflowStarter;
    private final TemplateBasedLocationsManager templateBasedLocationsManager;

    @ConstructorProperties({"requestValidator", "requestStorageManager", "swfWorkflowStarter", "templateBasedLocationsManager"})
    public DeleteMitigationFromAllLocationsActivity(@Nonnull RequestValidator requestValidator, @Nonnull RequestStorageManager requestStorageManager, 
                                                    @Nonnull SWFWorkflowStarter workflowStarter, @Nonnull TemplateBasedLocationsManager templateBasedLocationsManager) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestStorageManager);
        this.requestStorageManager = requestStorageManager;
        
        Validate.notNull(workflowStarter);
        this.workflowStarter = workflowStarter;
        
        Validate.notNull(templateBasedLocationsManager);
        this.templateBasedLocationsManager = templateBasedLocationsManager;
    }

    @Validated
    @Operation("DeleteMitigationFromAllLocations")
    @Documentation("DeleteMitigationFromAllLocations")
    public @Nonnull MitigationModificationResponse enact(@Nonnull DeleteMitigationFromAllLocationsRequest deleteRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "DeleteMitigationFromAllLocations.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("DeleteMitigationFromAllLocations called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(deleteRequest)));
            CommonActivityMetricsHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            String mitigationTemplate = deleteRequest.getMitigationTemplate();
            CommonActivityMetricsHelper.addTemplateNameCountMetrics(mitigationTemplate, tsdMetrics);
            
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            String deviceName = deviceNameAndScope.getDeviceName().name();
            CommonActivityMetricsHelper.addDeviceNameCountMetrics(deviceName, tsdMetrics);
            
            String deviceScope = deviceNameAndScope.getDeviceScope().name();
            CommonActivityMetricsHelper.addDeviceScopeCountMetrics(deviceScope, tsdMetrics);
            
            String serviceName = deleteRequest.getServiceName();
            CommonActivityMetricsHelper.addServiceNameCountMetrics(serviceName, tsdMetrics);

            // Step1. Validate this request.
            requestValidator.validateDeleteRequest(deleteRequest);
            
            // Step2. Validate this request based on the template.
            // No-op for now. There isn't anything we check based on the template for the delete requests, hence not invoking the templateBasedValidator here. 
            // If we ever do have an use-case for it, this would be the place to invoke the template-based request instance validator.
            
            // Step3. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(deleteRequest, RequestType.DeleteRequest, tsdMetrics);
            
            // Step4. Create new workflow client to be used for running the workflow.
            WorkflowClientExternal workflowClient = workflowStarter.createSWFWorkflowClient(workflowId, deleteRequest, deviceName, tsdMetrics);
            
            // Step5. Get the locations where we need to start running the workflow.
            // In most cases it is provided by the client, but for some templates we might have locations based on the templateName, 
            // hence checking with the templateBasedLocationsHelper and also passing it the original request to have the entire context.
            Set<String> locationsToDeploy = templateBasedLocationsManager.getLocationsForDeployment(deleteRequest);
            
            // Step6. Start running the workflow.
            workflowStarter.startWorkflow(workflowId, deleteRequest, locationsToDeploy, RequestType.DeleteRequest, 
                                          deleteRequest.getMitigationVersion(), deviceName, deviceScope, workflowClient, tsdMetrics);
            
            // Step7. Once the workflow is running, it should have an associated swfRunId, update the request record for this workflow request and store the associated runId.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.DeleteRequest, tsdMetrics);
            
            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(deleteRequest.getMitigationName());
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
            mitigationModificationResponse.setMitigationInstancesStatus(instanceStatuses);
            
            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage() + 
                                       " for request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.warn(msg, ex);
            tsdMetrics.addCount(DeleteExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (DuplicateRequestException400 ex) {
            String msg = String.format("Caught DuplicateDefinitionException in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage() + 
                                       " for request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.warn(msg, ex);
            tsdMetrics.addCount(DeleteExceptions.DuplicateRequest.name(), 1);
            throw ex;
        } catch (MissingMitigationException400 ex) {
            String msg = String.format("Caught MissingMitigationException in DeleteMitigationActivity for requestId: " + requestId + " with request: " + ", reason: " + ex.getMessage() + 
                                       " for request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.warn(msg, ex);
            tsdMetrics.addCount(DeleteExceptions.MissingMitigation.name(), 1);
            throw ex;
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for DeleteMitigationActivity: for requestId: " + requestId + ", reason: " + internalError.getMessage() + 
                                       " for request: " + ReflectionToStringBuilder.toString(deleteRequest));
            LOG.error(msg, internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(DeleteExceptions.InternalError.name(), 1);
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
