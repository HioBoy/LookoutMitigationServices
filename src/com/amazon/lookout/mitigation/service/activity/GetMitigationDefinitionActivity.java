package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatus;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

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
        MissingMitigation
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetMitigationDefinitionExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    private final RequestValidator requestValidator;
    private final RequestInfoHandler requestInfoHandler;
    private final MitigationInstanceInfoHandler mitigationInstanceHandler;
    
    @ConstructorProperties({"requestValidator", "requestInfoHandler", "mitigationInstanceHandler"})
    public GetMitigationDefinitionActivity(@NonNull RequestValidator requestValidator, @NonNull RequestInfoHandler requestInfoHandler, 
                                     @NonNull MitigationInstanceInfoHandler mitigationInstanceHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestInfoHandler);
        this.requestInfoHandler = requestInfoHandler;
        
        Validate.notNull(mitigationInstanceHandler);
        this.mitigationInstanceHandler = mitigationInstanceHandler;
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
            String deviceName = request.getDeviceName();
            String mitigationName = request.getMitigationName();
            int mitigationVersion = request.getMitigationVersion();

            LOG.info(String.format("GetMitigationDefinitionActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Step 1. Validate this request
            requestValidator.validateGetMitigationDefinitionRequest(request);
            
            // Step 2. get mitigation request for this device, mitigationName and mitigation version from the requests table.
            MitigationRequestDescription mitigationDescription = requestInfoHandler.getMitigationDefinition(deviceName, mitigationName, mitigationVersion, tsdMetrics);
            
            // Step 3. query the individual instance status and populate a new MitigationRequestDescriptionWithStatus instance to wrap this information.
            List<MitigationInstanceStatus> instanceStatuses = mitigationInstanceHandler.getMitigationInstanceStatus(deviceName, mitigationDescription.getJobId(), tsdMetrics);

            MitigationRequestDescriptionWithStatus mitigationRequestDescriptionWithStatus = new MitigationRequestDescriptionWithStatus();
            mitigationRequestDescriptionWithStatus.setMitigationRequestDescription(mitigationDescription);
            mitigationRequestDescriptionWithStatus.setInstancesStatus(instanceStatuses);
            
            // Step 4. Create the response object to return back to the client.
            GetMitigationDefinitionResponse response = new GetMitigationDefinitionResponse();
            response.setMitigationRequestDescriptionWithStatus(mitigationRequestDescriptionWithStatus);
            response.setRequestId(requestId);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetMitigationDefinitionActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationDefinitionExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (MissingMitigationException400 missingMitigationException) {
            String msg = "Caught MissingMitigationException in GetMitigationDefinitionActivity for requestId: " + requestId + ", reason: " + missingMitigationException.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), missingMitigationException);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationDefinitionExceptions.MissingMitigation.name(), 1);
            throw new MissingMitigationException400(msg);
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
