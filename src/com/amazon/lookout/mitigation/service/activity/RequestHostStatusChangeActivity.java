package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeRequest;
import com.amazon.lookout.mitigation.service.RequestHostStatusChangeResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.HostnameValidator;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import lombok.NonNull;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.ConstructorProperties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

@Service("LookoutMitigationService")
public class RequestHostStatusChangeActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(RequestHostStatusChangeActivity.class);

    private enum RequestHostStatusChangeExceptions {
        BadRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(RequestHostStatusChangeExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));

    private final RequestValidator requestValidator;
    private final HostnameValidator hostNameValidator;
    private final LocationStateInfoHandler locationStateInfoHandler;
    private final String localHostName;

    @ConstructorProperties({"requestValidator", "hostNameValidator", "locationStateInfoHandler"})
    public RequestHostStatusChangeActivity(@NonNull RequestValidator requestValidator,
                                           @NonNull HostnameValidator hostNameValidator,
                                           @NonNull LocationStateInfoHandler locationStateInfoHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(hostNameValidator);
        this.hostNameValidator = hostNameValidator;

        Validate.notNull(locationStateInfoHandler);
        this.locationStateInfoHandler = locationStateInfoHandler;

        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            LOG.warn("Failed to get local hostname!", ex);
            hostName = "unknown-lkt-mit-svc";
        }
        this.localHostName = hostName;
    }

    @Validated
    @Operation("RequestHostStatusChange")
    @Documentation("RequestHostStatusChange")
    public @NonNull RequestHostStatusChangeResponse enact(@NonNull RequestHostStatusChangeRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = false;

        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "RequestHostStatusChange.enact");
        try {
            LOG.info(String.format("RequestHostStatusChangeActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            // Step 1. Validate this request
            HostStatusEnum requestedStatus = requestValidator.validateRequestHostStatusChangeRequest(request);
            hostNameValidator.validateHostname(request);

            // Step 2. Request status change
            locationStateInfoHandler.requestHostStatusChange(
                    request.getLocation().toLowerCase(),
                    request.getHostName(),
                    requestedStatus,
                    request.getReason(),
                    request.getUserName(),
                    localHostName,
                    request.getRelatedLinks(),
                    tsdMetrics);

            // Step 3. Create and return result
            RequestHostStatusChangeResponse response = new RequestHostStatusChangeResponse();
            response.setRequestId(requestId);
            return response;

        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "RequestHostStatusChangeActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RequestHostStatusChangeExceptions.BadRequest.name(), 1);
            requestSuccessfullyProcessed = true;
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error in RequestHostStatusChangeActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                    " for request: " + ReflectionToStringBuilder.toString(request), internalError);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + RequestHostStatusChangeExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
