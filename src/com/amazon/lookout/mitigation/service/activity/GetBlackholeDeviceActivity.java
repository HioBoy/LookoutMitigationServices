package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.service.Activity;
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.GetBlackholeDeviceResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackholeDeviceConverter;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;

@Service("LookoutMitigationService")
public class GetBlackholeDeviceActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetBlackholeDeviceActivity.class);
    
    private enum GetBlackholeDeviceActivityExceptions {
        BadRequest,
        StaleRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetBlackholeDeviceActivityExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));

    @NonNull private final RequestValidator requestValidator;
    @NonNull private final BlackholeMitigationHelper blackholeMitigationHelper;
    
    @ConstructorProperties({"requestValidator", "blackholeMitigationHelper"})
    public GetBlackholeDeviceActivity(@Nonnull RequestValidator requestValidator, 
            @Nonnull BlackholeMitigationHelper blackholeMitigationHelper) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;

        Validate.notNull(blackholeMitigationHelper);
        this.blackholeMitigationHelper = blackholeMitigationHelper;
    }

    @Operation("GetBlackholeDevice")
    @Documentation("API for getting a blackhole devices by name")
    public GetBlackholeDeviceResponse enact(GetBlackholeDeviceRequest request)
            throws InternalServerError500, BadRequest400
    {
        getMetrics().addProperty("Name", request.getName());
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetBlackholeDevice.enact");
        try {
            CommonActivityMetricsHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            Optional<BlackholeDevice> device = blackholeMitigationHelper.getBlackholeDevice(request.getName(), tsdMetrics);
            if (!device.isPresent()) {
                throw new BadRequest400("Device " + request.getName() + " does not exist");
            }
            
            GetBlackholeDeviceResponse response = new GetBlackholeDeviceResponse();
            response.setDevice(BlackholeDeviceConverter.convertBlackholeDeviceResponse(device.get()));
            response.setRequestId(requestId);
            
            tsdMetrics.addProperty("Version", String.valueOf(device.get().getVersion()));
            
            return response;
        } catch (BadRequest400 badrequest) {
            throw badrequest;
        } catch (Exception internalError) {
            String msg = "Internal error in GetBlackholeDeviceActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetBlackholeDeviceActivityExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
