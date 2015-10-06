package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.NonNull;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.activities.model.ActiveMitigationDetails;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.OperationNotSupportedException400;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActiveMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Activity to mark a location as inactive. This activity currently (03/2015) is supported to only run in non-prod domains.
 * Once invoked, this activity fetches list of mitigations deployed to this location (for the given service+device). For each of those mitigations,
 * this activity then marks those as defunct for this particular location.
 * 
 * Once a mitigation at a location is marked as defunct, the ActiveMitigations table will no longer show the mitigation as active for that location.
 * A separate process in the ActivityWorkers is responsible for marking an entire workflow (request) as defunct if there are no active instances for it - 
 * hence this Activity doesn't need to work on the status of the workflows (requests) which had created the mitigation which is now marked as defunct.
 * Note: Context for the above reference to working on requests - in some templates, we only allow 1 active mitigation to exist. For such templates, it is 
 *       important for the workflow (request) to be marked as inactive if all of the locations where its previous deployment was carried out is no longer active.
 *
 */
@ThreadSafe
@Service("LookoutMitigationService")
public class ReportInactiveLocationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ReportInactiveLocationActivity.class);

    private enum ReportInactiveLocationExceptions {
        BadRequest,
        InternalError,
        OperationNotSupported
    }

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(ReportInactiveLocationExceptions.BadRequest.name(),
                                                                                                      ReportInactiveLocationExceptions.InternalError.name(),
                                                                                                      ReportInactiveLocationExceptions.OperationNotSupported.name()));

    private static final String PROD_DOMAIN = "prod";
    private static final String SERVICE_NAME_KEY = "InactiveForService";
    private static final String DEVICE_NAME_KEY = "InactiveForDevice";
    private static final String LOCATION_NAME_KEY = "InactiveForLocation";
    private static final String NUM_ACTIVE_MITIGATIONS_FOUND = "NumActiveMitigations";
    private static final String NUM_MITIGATIONS_MARKED_DEFUNCT = "NumMitigationsMarkedDefunct";

    @NonNull private final RequestValidator requestValidator;
    @NonNull private final ActiveMitigationInfoHandler activeMitigationInfoHandler;
    @NonNull private final String domain;

    @ConstructorProperties({"requestValidator", "activeMitigationInfoHandler", "domain"})
    public ReportInactiveLocationActivity(@NonNull RequestValidator requestValidator, @NonNull ActiveMitigationInfoHandler activeMitigationInfoHandler, @NonNull String domain) {
        this.requestValidator = requestValidator;
        this.activeMitigationInfoHandler = activeMitigationInfoHandler;

        Validate.notBlank(domain);
        this.domain = domain;
    }

    @Validated
    @Operation("ReportInactiveLocation")
    @Documentation("ReportInactiveLocation")
    public @NonNull ReportInactiveLocationResponse enact(@NonNull ReportInactiveLocationRequest request) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ReportInactiveLocation.enact");

        boolean requestSuccessfullyProcessed = true;

        String requestId = getRequestId().toString();

        String deviceName = request.getDeviceName();
        tsdMetrics.addProperty(DEVICE_NAME_KEY, deviceName);

        String serviceName = request.getServiceName();
        tsdMetrics.addProperty(SERVICE_NAME_KEY, serviceName);

        String location = request.getLocation();
        tsdMetrics.addProperty(LOCATION_NAME_KEY, location);

        String requestDesc = "Location: " + location + ", service: " + serviceName + ", device: " + deviceName;

        try {
            LOG.info("ReportInactiveLocationActivity called with RequestId: " + requestId + " and Request: " + requestDesc);
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);

            requestValidator.validateReportInactiveLocation(request);

            if (domain.equalsIgnoreCase(PROD_DOMAIN)) {
                String msg = "ReportInactiveLocation is not supported for domain: " + domain + ". Request: " + requestDesc + " RequestId: " + requestId;
                LOG.warn(msg);
                throw new OperationNotSupportedException400(msg);
            }

            List<ActiveMitigationDetails> activeMitigationDetails = activeMitigationInfoHandler.getActiveMitigationsForService(serviceName, deviceName, Lists.newArrayList(location), tsdMetrics);
            tsdMetrics.addCount(NUM_ACTIVE_MITIGATIONS_FOUND, activeMitigationDetails.size());

            List<String> mitigationsMarkedAsDefunct = new ArrayList<>();
            for (ActiveMitigationDetails activeMitigationInfo : activeMitigationDetails) {
                String mitigationName = activeMitigationInfo.getMitigationName();
                try {
                    activeMitigationInfoHandler.markMitigationAsDefunct(serviceName, mitigationName, deviceName, location, activeMitigationInfo.getJobId(), 
                                                                        activeMitigationInfo.getLastDeployDate(), tsdMetrics);
                    mitigationsMarkedAsDefunct.add(mitigationName);
                    tsdMetrics.addOne(NUM_MITIGATIONS_MARKED_DEFUNCT);
                } catch (Exception ex) {
                    String msg = "Caught exception when marking mitigation: " + mitigationName + " defunct for request: " + requestDesc + 
                                 ". Mitigations marked as defunct so far: " + mitigationsMarkedAsDefunct;
                    LOG.warn(msg, ex);
                    throw new InternalServerError500(msg);
                }
            }

            ReportInactiveLocationResponse response = new ReportInactiveLocationResponse();
            response.setDeviceName(deviceName);
            response.setLocation(location);
            response.setMitigationsMarkedAsDefunct(mitigationsMarkedAsDefunct);
            response.setRequestId(requestId);
            response.setServiceName(serviceName);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "ReportInactiveLocationActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ReportInactiveLocationExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg);
        } catch (OperationNotSupportedException400 ex) {
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ReportInactiveLocationExceptions.OperationNotSupported.name(), 1);
            throw ex;
        } catch (Exception internalError) {
            String msg = "Internal error in ReportInactiveLocationActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ReportInactiveLocationExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
