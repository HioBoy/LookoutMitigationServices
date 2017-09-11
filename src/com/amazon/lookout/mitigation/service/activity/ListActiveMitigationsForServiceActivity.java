package com.amazon.lookout.mitigation.service.activity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.AllArgsConstructor;
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
import com.amazon.lookout.mitigation.ActiveMitigationsHelper;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceResponse;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ListActiveMitigationsForServiceActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListActiveMitigationsForServiceActivity.class);

    private enum ListActiveMitigationsExceptions {
        BadRequest,
        InternalError
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(ListActiveMitigationsExceptions.BadRequest.name(),
                                                                                                      ListActiveMitigationsExceptions.InternalError.name()));
    @NonNull private final RequestValidator requestValidator;
    @NonNull private final ActiveMitigationsHelper activeMitigationsHelper;

    public static final String KEY_SEPARATOR = "#";

    @Validated
    @Operation("ListActiveMitigationsForService")
    @Documentation("ListActiveMitigationsForService")
    public @NonNull ListActiveMitigationsForServiceResponse enact(@NonNull ListActiveMitigationsForServiceRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ListActiveMitigationsForService.enact");

        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;

        try {            
            LOG.info(String.format("ListActiveMitigationsForServiceActivity called with RequestId: "
                    + "%s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step 1. Validate your request.
            requestValidator.validateListActiveMitigationsForServiceRequest(request);

            String deviceName = request.getDeviceName();
            final String location = request.getLocation();

            // Step 2. Get list of Active Mitigations by looking at the current symlink S3 object
            List<MitigationRequestDescriptionWithStatuses> requestDescriptionsToReturn = 
                    activeMitigationsHelper.getActiveMitigations(DeviceName.valueOf(deviceName), location);

            // Step 3. Generate response
            ListActiveMitigationsForServiceResponse response = new ListActiveMitigationsForServiceResponse();
            response.setServiceName(request.getServiceName());
            response.setRequestId(requestId);
            response.setMitigationRequestDescriptionsWithStatuses(requestDescriptionsToReturn);

            LOG.info(String.format("ListMitigationsActivity called with RequestId: %s returned: %s.",
                    requestId, ReflectionToStringBuilder.toString(response)));  
            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, 
                    requestId, "ListActiveMitigationsForServiceActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + 
                    ListActiveMitigationsExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in ListActiveMitigationsForServiceActivity for requestId: " + 
                    requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListActiveMitigationsExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}