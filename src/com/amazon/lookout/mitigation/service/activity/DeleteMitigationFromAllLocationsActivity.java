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
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;

import com.amazon.lookout.mitigation.RequestCreator;

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
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(DeleteExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    @NonNull private final RequestCreator requestCreator;

    @ConstructorProperties({"requestValidator", "templateBasedValidator", "requestCreator"})
    public DeleteMitigationFromAllLocationsActivity(@NonNull RequestValidator requestValidator,
            @NonNull TemplateBasedRequestValidator templateBasedValidator,
            @NonNull final RequestCreator requestCreator) {
        this.requestValidator = requestValidator;
        this.templateBasedValidator = templateBasedValidator;
        this.requestCreator = requestCreator;
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
            
            final DeviceName device = DeviceName.valueOf(deleteRequest.getDeviceName());
            ActivityHelper.addDeviceNameCountMetrics(device.name(), tsdMetrics);
            
            // Validate this request.
            requestValidator.validateDeleteRequest(deleteRequest);
            
            // Validate this request based on the template.
            templateBasedValidator.validateRequestForTemplate(deleteRequest, tsdMetrics);

            final String location = deleteRequest.getLocation();

            final MitigationModificationResponse response = requestCreator.queueRequest(
                    deleteRequest, RequestType.DeleteRequest, null);

            // TODO: just return the response, instead of doing this
            final long workflowId = response.getJobId();
            final int storedMitigationVersion = response.getMitigationVersion();

            // Return the workflowId to the client.
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(deleteRequest.getMitigationName());
            mitigationModificationResponse.setMitigationVersion(storedMitigationVersion);
            mitigationModificationResponse.setMitigationTemplate(deleteRequest.getMitigationTemplate());
            mitigationModificationResponse.setDeviceName(device.name());
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
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "DeleteMitigationFromAllLocationsActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(deleteRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (DuplicateRequestException400 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(deleteRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + DeleteExceptions.DuplicateRequest.name(), 1);
            throw new DuplicateRequestException400(msg);
        } catch (MissingMitigationException400 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in DeleteMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
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

