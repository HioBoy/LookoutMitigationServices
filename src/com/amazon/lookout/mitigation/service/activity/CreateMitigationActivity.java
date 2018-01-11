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
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.DuplicateMitigationNameException400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;

import com.amazon.lookout.mitigation.RequestCreator;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(CreateMitigationActivity.class);
    
    private enum CreateExceptions {
        BadRequest,
        DuplicateName,
        DuplicateDefinition,
        InternalError
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(CreateExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    private final RequestValidator requestValidator;
    private final TemplateBasedRequestValidator templateBasedValidator;
    @NonNull private final RequestCreator requestCreator;
    
    @ConstructorProperties({"requestValidator", "templateBasedValidator", "requestCreator"})
    public CreateMitigationActivity(@NonNull RequestValidator requestValidator,
            @NonNull TemplateBasedRequestValidator templateBasedValidator,
            @NonNull final RequestCreator requestCreator) {
        this.requestValidator = requestValidator;
        this.templateBasedValidator = templateBasedValidator;
        this.requestCreator = requestCreator;
    }

    @Validated
    @Operation("CreateMitigation")
    @Documentation("CreateMitigation")
    public @NonNull MitigationModificationResponse enact(@NonNull CreateMitigationRequest createRequest) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "CreateMitigation.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try {
            LOG.debug(String.format("CreateMitigationActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(createRequest)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            String mitigationTemplate = createRequest.getMitigationTemplate();
            ActivityHelper.addTemplateNameCountMetrics(mitigationTemplate, tsdMetrics);
            
            final DeviceName device = DeviceName.valueOf(createRequest.getDeviceName());
            ActivityHelper.addDeviceNameCountMetrics(device.name(), tsdMetrics);

            // Validate this request.
            requestValidator.validateCreateRequest(createRequest);
            
            // Validate this request based on the template.
            templateBasedValidator.validateRequestForTemplate(createRequest, tsdMetrics);
            
            final String location = createRequest.getLocation();

            final MitigationModificationResponse response = requestCreator.queueRequest(
                    createRequest, RequestType.CreateRequest,
                    createRequest.getMitigationDefinition());

            final long workflowId = response.getJobId();
            final int storedMitigationVersion = response.getMitigationVersion();

            // Return the workflowId to the client.
            // TODO just return the response, instead of doing this
            MitigationModificationResponse mitigationModificationResponse = new MitigationModificationResponse();
            mitigationModificationResponse.setMitigationName(createRequest.getMitigationName());
            mitigationModificationResponse.setMitigationVersion(storedMitigationVersion);
            mitigationModificationResponse.setMitigationTemplate(createRequest.getMitigationTemplate());
            mitigationModificationResponse.setDeviceName(device.name());
            mitigationModificationResponse.setJobId(workflowId);
            mitigationModificationResponse.setRequestStatus(WorkflowStatus.RUNNING);
            
            // TODO we support only one location.  Don't return a list.
            List<MitigationInstanceStatus> instanceStatuses = new ArrayList<>();
            MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
            instanceStatus.setLocation(location);
            instanceStatus.setMitigationStatus(MitigationStatus.CREATED);
            instanceStatuses.add(instanceStatus);
            mitigationModificationResponse.setMitigationInstanceStatuses(instanceStatuses);
            
            return mitigationModificationResponse;
        } catch (IllegalArgumentException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "CreateMitigationActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(createRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (DuplicateMitigationNameException400 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in CreateMitigationActivity for requestId: "
                    + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(createRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateExceptions.DuplicateName.name(), 1);
            throw new DuplicateMitigationNameException400(msg);
        } catch (DuplicateDefinitionException400 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in CreateMitigationActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(createRequest), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateExceptions.DuplicateDefinition.name(), 1);
            throw new DuplicateDefinitionException400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in CreateMitigationActivity: for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(createRequest),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
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

