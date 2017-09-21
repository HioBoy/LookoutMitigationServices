package com.amazon.lookout.mitigation.service.activity;

import org.apache.commons.lang3.Validate;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
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
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

import com.amazon.lookout.mitigation.RequestCreator;
import com.amazon.lookout.mitigation.datastore.SwitcherooDAO;

@ThreadSafe
@Service("LookoutMitigationService")
public class EditMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(EditMitigationActivity.class);

    private enum EditExceptions {
        BadRequest,
        StaleRequest,
        DuplicateDefinition,
        MissingMitigation,
        InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(EditExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet())); 

    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    private final RequestStorageManager requestStorageManager;
    private final SWFWorkflowStarter swfWorkflowStarter;
    private final TemplateBasedLocationsManager templateBasedLocationsManager;
    @NonNull private final RequestCreator requestCreator;
    @NonNull private final SwitcherooDAO switcherooDao;

    @ConstructorProperties({"requestValidator", "templateBasedValidator",
    "requestStorageManager", "swfWorkflowStarter", "templateBasedLocationsManager",
    "requestCreator", "switcherooDao"})
    public EditMitigationActivity(@NonNull RequestValidator requestValidator,
            @NonNull TemplateBasedRequestValidator templateBasedValidator,
            @NonNull RequestStorageManager requestStorageManager,
            @NonNull SWFWorkflowStarter swfWorkflowStarter,
            @NonNull TemplateBasedLocationsManager templateBasedLocationsManager,
            @NonNull final RequestCreator requestCreator,
            @NonNull final SwitcherooDAO switcherooDao) {
        this.requestValidator = requestValidator;
        this.templateBasedValidator = templateBasedValidator;
        this.requestStorageManager = requestStorageManager;
        this.swfWorkflowStarter = swfWorkflowStarter;
        this.templateBasedLocationsManager = templateBasedLocationsManager;
        this.requestCreator = requestCreator;
        this.switcherooDao = switcherooDao;
    }

    @Validated
    @Operation("EditMitigation")
    @Documentation("EditMitigation")
    public @NonNull MitigationModificationResponse enact(@NonNull EditMitigationRequest editRequest) {

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "EditMitigation.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {
            LOG.debug(String.format("EditMitigationActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(editRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            String mitigationTemplate = editRequest.getMitigationTemplate(); 
            ActivityHelper.addTemplateNameCountMetrics(mitigationTemplate, tsdMetrics);

            String deviceName = editRequest.getDeviceName();
            final DeviceName device = DeviceName.valueOf(deviceName);  // A real typed device
            ActivityHelper.addDeviceNameCountMetrics(deviceName, tsdMetrics);

            String serviceName = editRequest.getServiceName();
            ActivityHelper.addServiceNameCountMetrics(serviceName, tsdMetrics);

            // Step1. Validate this request.
            requestValidator.validateEditRequest(editRequest);

            // Step2. Validate this request based on the template.
            templateBasedValidator.validateRequestForTemplate(editRequest, tsdMetrics);

            final String location = editRequest.getLocation();

            final long workflowId;
            final int storedMitigationVersion;

            if (switcherooDao.useNewMitigationService(device, location)) {
                LOG.info(String.format("Using new mitigation service for request %s", requestId));

                final MitigationModificationResponse response = requestCreator.queueRequest(
                        editRequest, RequestType.EditRequest,
                        editRequest.getMitigationDefinition());

                // TODO: just return the response, instead of doing this
                workflowId = response.getJobId();
                storedMitigationVersion = response.getMitigationVersion();
            } else {
                // Use location from the request
                final Set<String> locationsToDeploy = new HashSet<>();
                locationsToDeploy.add(location);

                // Step4. Persist this request in DDB and get back the workflowId associated with this request.
                RequestStorageResponse requestStorageResponse = requestStorageManager.storeRequestForWorkflow(
                        editRequest, locationsToDeploy, RequestType.EditRequest, tsdMetrics);
                
                workflowId = requestStorageResponse.getWorkflowId();
                storedMitigationVersion = requestStorageResponse.getMitigationVersion();

                // Step5. Create new workflow client to be used for running the workflow.
                WorkflowClientExternal workflowClient = swfWorkflowStarter.createMitigationModificationWorkflowClient(workflowId, editRequest, deviceName, tsdMetrics);

                // Step6. Start running the workflow.
                swfWorkflowStarter.startMitigationModificationWorkflow(workflowId, editRequest, locationsToDeploy, RequestType.EditRequest,
                        storedMitigationVersion, deviceName, workflowClient, tsdMetrics);

                // Step7. Update the record for this workflow request and store the runId that SWF associates with this workflow.
                String swfRunId = workflowClient.getWorkflowExecution().getRunId();
                requestStorageManager.updateRunIdForWorkflowRequest(deviceName, workflowId, swfRunId, RequestType.EditRequest, tsdMetrics);
            }

            // Step8. Return back the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(editRequest.getMitigationName());
            mitigationModificationResponse.setMitigationVersion(storedMitigationVersion);
            mitigationModificationResponse.setMitigationTemplate(editRequest.getMitigationTemplate());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(serviceName);
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.RUNNING);

            // TODO we only support one location, don't return a list
            List<MitigationInstanceStatus> instanceStatuses = new ArrayList<>();
            MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
            instanceStatus.setLocation(location);
            instanceStatus.setMitigationStatus(MitigationStatus.CREATED);
            instanceStatuses.add(instanceStatus);
            mitigationModificationResponse.setMitigationInstanceStatuses(instanceStatuses);

            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String message = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "EditMitigationActivity", ex.getMessage());
            LOG.warn(message + " for request: " + ReflectionToStringBuilder.toString(editRequest), ex);
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.BadRequest.name());
            throw new BadRequest400(message);
        } catch (StaleRequestException400 ex) {
            LOG.warn(String.format("Caught " + ex.getClass().getSimpleName() + " in EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(editRequest)), ex);
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.StaleRequest.name());
            throw ex;
        } catch (DuplicateDefinitionException400 ex) {
            LOG.warn(String.format("Caught " + ex.getClass().getSimpleName() + " in EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(editRequest)), ex);
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.DuplicateDefinition.name());
            throw ex;
        } catch (MissingMitigationException400 ex) {
            LOG.warn(String.format("Caught " + ex.getClass().getSimpleName() + " in EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, ex.getMessage(), 
                    ReflectionToStringBuilder.toString(editRequest)), ex);
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.MissingMitigation.name());
            throw ex;
        } catch (Exception internalError) {
            String errMsg = String.format("Internal error while fulfilling request for EditMitigationActivity for requestId: %s, reason: %s for request: %s", requestId, internalError.getMessage(),
                    ReflectionToStringBuilder.toString(editRequest));
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + errMsg, internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + EditExceptions.InternalError.name());
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

