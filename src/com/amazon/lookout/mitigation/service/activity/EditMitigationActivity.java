package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
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
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
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
public class EditMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(EditMitigationActivity.class);

    private enum EditExceptions {
        BadRequest,
        StaleRequest,
        DuplicateDefinition,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(EditExceptions.BadRequest.name(),
            EditExceptions.StaleRequest.name(),
            EditExceptions.DuplicateDefinition.name(),
            EditExceptions.InternalError.name())); 

    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter swfWorkflowStarter;
    private final TemplateBasedLocationsManager templateBasedLocationsManager;

    @ConstructorProperties({"requestValidator", "templateBasedValidator", "requestStorageManager", "swfWorkflowStarter", "templateBasedLocationsManager"})
    public EditMitigationActivity(@Nonnull RequestValidator requestValidator, @Nonnull TemplateBasedRequestValidator templateBasedValidator, 
            @Nonnull RequestStorageManager requestStorageManager, @Nonnull SWFWorkflowStarter swfWorkflowStarter,
            @Nonnull TemplateBasedLocationsManager templateBasedLocationsManager) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(templateBasedValidator);
        this.templateBasedValidator = templateBasedValidator;

        Validate.notNull(requestStorageManager);
        this.requestStorageManager = requestStorageManager;

        Validate.notNull(swfWorkflowStarter);
        this.swfWorkflowStarter = swfWorkflowStarter;

        Validate.notNull(templateBasedLocationsManager);
        this.templateBasedLocationsManager = templateBasedLocationsManager;
    }

    @Validated
    @Operation("EditMitigation")
    @Documentation("EditMitigation")
    public @Nonnull MitigationModificationResponse enact(@Nonnull EditMitigationRequest editRequest) {

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "EditMitigation.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            LOG.debug(String.format("EditMitigationActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(editRequest)));
            CommonActivityMetricsHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            String mitigationTemplate = editRequest.getMitigationTemplate(); 
            CommonActivityMetricsHelper.addTemplateNameCountMetrics(mitigationTemplate, tsdMetrics);

            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            String deviceName = deviceNameAndScope.getDeviceName().name();
            CommonActivityMetricsHelper.addDeviceNameCountMetrics(deviceName, tsdMetrics);
            String deviceScope = deviceNameAndScope.getDeviceScope().name();
            CommonActivityMetricsHelper.addDeviceScopeCountMetrics(deviceScope, tsdMetrics);

            String serviceName = editRequest.getServiceName();
            CommonActivityMetricsHelper.addServiceNameCountMetrics(serviceName, tsdMetrics);

            // Step1. Validate this request.
            requestValidator.validateEditRequest(editRequest);

            // Step2. Validate this request based on the template.
            templateBasedValidator.validateRequestForTemplate(editRequest, tsdMetrics);

            // Step3. Get the locations where we need to start running the workflow.
            // In most cases it is provided by the client, but for some templates we might have locations based on the templateName, 
            // hence checking with the templateBasedLocationsHelper and also passing it the original request to have the entire context.
            Set<String> locationsToDeploy = templateBasedLocationsManager.getLocationsForDeployment(editRequest);

            // Step4. Persist this request in DDB and get back the workflowId associated with this request.
            long workflowId = requestStorageManager.storeRequestForWorkflow(editRequest, locationsToDeploy, RequestType.EditRequest, tsdMetrics);

            // Step5. Create new workflow client to be used for running the workflow.
            WorkflowClientExternal workflowClient = swfWorkflowStarter.createMitigationModificationWorkflowClient(workflowId, editRequest, deviceName, tsdMetrics);

            // Step6. Start running the workflow.
            swfWorkflowStarter.startMitigationModificationWorkflow(workflowId, editRequest, locationsToDeploy, RequestType.EditRequest,
                    editRequest.getMitigationVersion(), deviceName, deviceScope, workflowClient, tsdMetrics);

            // Step7. Update the record for this workflow request and store the runId that SWF associates with this workflow.
            String swfRunId = workflowClient.getWorkflowExecution().getRunId();
            requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.EditRequest, tsdMetrics);

            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(editRequest.getMitigationName());
            mitigationModificationResponse.setMitigationTemplate(editRequest.getMitigationTemplate());
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
        } catch (IllegalArgumentException ex) {
            String message = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "EditMitigationActivity", ex.getMessage());
            LOG.warn(message + " for request: " + ReflectionToStringBuilder.toString(editRequest), ex);
            tsdMetrics.addOne(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.BadRequest.name());
            throw new BadRequest400(message);
        } catch (StaleRequestException400 ex) {
            LOG.warn(String.format("Caught StaleRequestException in EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(editRequest)), ex);
            tsdMetrics.addOne(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.StaleRequest.name());
            throw ex;
        } catch (DuplicateDefinitionException400 ex) {
            LOG.warn(String.format("Caught DuplicateDefinitionException400 in EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(editRequest)), ex);
            tsdMetrics.addOne(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.DuplicateDefinition.name());
            throw ex;
        } catch (Exception internalError) {
            String errMsg = String.format("Internal error while fulfilling request for EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, internalError.getMessage(),
                    ReflectionToStringBuilder.toString(editRequest));
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + errMsg, internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addOne(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.InternalError.name());
            throw new InternalServerError500(errMsg);
        } finally {
            if (requestSuccessfullyProcessed) {
                tsdMetrics.addOne(LookoutMitigationServiceConstants.ENACT_SUCCESS);
                tsdMetrics.addZero(LookoutMitigationServiceConstants.ENACT_FAILURE);
            } else {
                tsdMetrics.addZero(LookoutMitigationServiceConstants.ENACT_SUCCESS);
                tsdMetrics.addOne(LookoutMitigationServiceConstants.ENACT_FAILURE);
            }
            tsdMetrics.end();
        }
    } 
}
