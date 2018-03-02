package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.NonNull;
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
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatus;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.mitigation.service.constants.DeviceName;

import com.amazon.lookout.mitigation.datastore.model.CurrentRequest;
import com.amazon.lookout.mitigation.datastore.model.RequestPage;
import com.amazon.lookout.mitigation.datastore.RequestsDAO;

@Service("LookoutMitigationService")
public class GetMitigationInfoActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetMitigationInfoActivity.class);
    
    private enum GetMitigationInfoExceptions {
        BadRequest,
        InternalError,
        MissingMitigation
    }
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(GetMitigationInfoExceptions.BadRequest.name(),
                                                                                                      GetMitigationInfoExceptions.InternalError.name())); 

    private final RequestValidator requestValidator;
    @NonNull private final RequestsDAO requestsDao;
    
    @ConstructorProperties({"requestValidator", "requestsDao"})
    public GetMitigationInfoActivity(@NonNull RequestValidator requestValidator,
            @NonNull final RequestsDAO requestsDao) {
        this.requestValidator = requestValidator;
        this.requestsDao = requestsDao;
    }

    @Validated
    @Operation("GetMitigationInfo")
    @Documentation("GetMitigationInfo")
    public @NonNull GetMitigationInfoResponse enact(@NonNull GetMitigationInfoRequest request) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetMitigationInfo.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        String mitigationName = request.getMitigationName();
        final String location = request.getLocation();

        try {            
            // A real scoped device
            final DeviceName device = DeviceName.valueOf(request.getDeviceName());

            LOG.info(String.format("GetMitigationInfoActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Validate this request
            requestValidator.validateGetMitigationInfoRequest(request);

            List<MitigationRequestDescriptionWithStatus> mitigationDescriptionWithStatuses = new ArrayList<>();

            // Get the most recent request for this mitigation that is not a delete
            CurrentRequest lastRequest = null;
            String lastEvaluatedKey = null;

            do {
                final RequestPage<CurrentRequest> page = requestsDao.getMitigationRequestsPage(
                        device, location, mitigationName, 100, lastEvaluatedKey);
                lastEvaluatedKey = page.getLastEvaluatedKey();

                // Results are returned most recent first, take the first one
                for (CurrentRequest c : page.getPage()) {
                    if (c.getRequestType() != RequestType.DeleteRequest) {
                        lastRequest = c;
                        break;
                    }
                }
            } while (lastRequest == null && lastEvaluatedKey != null);

            if (lastRequest == null) {
                final String msg = String.format("Mitigation %s does not exist on %s/%s",
                        mitigationName, device.name(), location);
                throw new MissingMitigationException400(msg);
            }

            mitigationDescriptionWithStatuses.add(
                    lastRequest.asMitigationRequestDescriptionWithStatus());

            GetMitigationInfoResponse response = new GetMitigationInfoResponse();
            response.setDeviceName(device.name());
            response.setMitigationName(mitigationName);
            response.setMitigationRequestDescriptionsWithStatus(mitigationDescriptionWithStatuses);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetMitigationInfoActivity", ex.getMessage());
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationInfoExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (MissingMitigationException400 ex) {
            String msg = "Caught " + ex.getClass().getSimpleName() + " in GetMitigationInfoActivity for requestId: " + requestId + ", reason: " + ex.getMessage();
            LOG.warn(msg + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationInfoExceptions.MissingMitigation.name(), 1);
            throw new MissingMitigationException400(msg);
        } catch (Exception internalError) {
            String msg = "Internal error in GetMitigationInfoActivity for requestId: " + requestId + ", reason: " + internalError.getMessage(); 
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg +
                            " for request: " + ReflectionToStringBuilder.toString(request),
                    internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationInfoExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}

