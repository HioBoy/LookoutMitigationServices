package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionRequest;
import com.amazon.lookout.mitigation.service.GetMitigationDefinitionResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationVersionException404;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocationAndStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;

import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

/**
 * Activity to get mitigation definition based on device name, mitigation name and mitigation version
 * This API queries GSI, result is eventually consistent.
 */
@Service("LookoutMitigationService")
public class GetMitigationDefinitionActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetMitigationDefinitionActivity.class);
    
    private enum GetMitigationDefinitionExceptions {
        BadRequest,
        InternalError,
        MissingMitigationVersion
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetMitigationDefinitionExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final RequestsDAO requestsDao;

    @ConstructorProperties({"requestValidator", "requestsDao"})
    public GetMitigationDefinitionActivity(@NonNull RequestValidator requestValidator,
            @NonNull final RequestsDAO requestsDao) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
    }

    @Validated
    @Operation("GetMitigationDefinition")
    @Documentation("GetMitigationDefinition")
    public @NonNull GetMitigationDefinitionResponse enact(@NonNull GetMitigationDefinitionRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
       
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetMitigationDefinition.enact");
        try {            
            final String mitigationName = request.getMitigationName();
            final int mitigationVersion = request.getMitigationVersion();
            final DeviceName device = DeviceName.valueOf(request.getDeviceName());
            final String location = request.getLocation();

            LOG.info(String.format("GetMitigationDefinitionActivity called with RequestId: %s and Request: %s.",
                    requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateGetMitigationDefinitionRequest(request);

            // Find the request
            final CurrentRequest currentRequest = requestsDao.retrieveRequestByMitigation(
                    device, location, mitigationName, mitigationVersion);

            if (currentRequest == null) {
                final String msg = String.format("Unable to locate request with "
                        + "device %s, location %s, mitigation %s, version %d",
                        device.name(), location, mitigationName, mitigationVersion);
                throw new MissingMitigationVersionException404(msg);
            }

            // Turn the CurrentRequest into whatever insane structure the API demands
            final MitigationRequestDescriptionWithLocations mitigationDescriptionWithLocations =
                currentRequest.asMitigationRequestDescriptionWithLocations();

            // The instance statuses are redundantly redundant
            final List<MitigationInstanceStatus> instanceStatuses = currentRequest.getMitigationInstanceStatuses();

            MitigationRequestDescriptionWithLocationAndStatus mitigationRequestDescriptionWithLocationAndStatus = 
                    new MitigationRequestDescriptionWithLocationAndStatus();
            mitigationRequestDescriptionWithLocationAndStatus.setMitigationRequestDescriptionWithLocations(mitigationDescriptionWithLocations);
            mitigationRequestDescriptionWithLocationAndStatus.setInstancesStatus(instanceStatuses);
            
            // Create the response object to return to the client.
            GetMitigationDefinitionResponse response = new GetMitigationDefinitionResponse();
            response.setMitigationRequestDescriptionWithLocationAndStatus(mitigationRequestDescriptionWithLocationAndStatus);
            response.setRequestId(requestId);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId,
                    "GetMitigationDefinitionActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationDefinitionExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (MissingMitigationVersionException404 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in GetMitigationDefinitionActivity for requestId: " + requestId
                    + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationDefinitionExceptions.MissingMitigationVersion.name(), 1);
            throw new MissingMitigationVersionException404(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in GetMitigationDefinitionActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationDefinitionExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

