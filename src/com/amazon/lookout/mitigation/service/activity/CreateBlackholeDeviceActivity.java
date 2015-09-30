package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
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
import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.BlackholeDeviceInfo;
import com.amazon.lookout.mitigation.service.CreateBlackholeDeviceRequest;
import com.amazon.lookout.mitigation.service.CreateBlackholeDeviceResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.StaleRequestException400;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackholeDeviceConverter;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

@ThreadSafe
@Service("LookoutMitigationService")
public class CreateBlackholeDeviceActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(CreateBlackholeDeviceActivity.class);

    private enum CreateBlackholeDeviceExceptions {
        BadRequest,
        StaleRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(CreateBlackholeDeviceExceptions.values())
                  .map(e -> e.name())
                  .collect(Collectors.toSet()));

    @NonNull private final RequestValidator requestValidator;
    @NonNull private final BlackholeMitigationHelper blackholeMitigationHelper;
    
    @ConstructorProperties({"requestValidator", "blackholeMitigationHelper"})
    public CreateBlackholeDeviceActivity(@Nonnull RequestValidator requestValidator, 
            @Nonnull BlackholeMitigationHelper blackholeMitigationHelper) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(blackholeMitigationHelper);
        this.blackholeMitigationHelper = blackholeMitigationHelper;
    }

    @Validated
    @Operation("CreateBlackholeDevice")
    @Documentation("CreateBlackholeDevice")
    public @Nonnull CreateBlackholeDeviceResponse enact(@Nonnull CreateBlackholeDeviceRequest request) {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "CreateBlackholeDevice.enact");
        try { 
            LOG.info(String.format("CreateBlackholeDeviceActivity called with RequestId: %s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            BlackholeDeviceInfo blackholeDeviceInfo = request.getBlackholeDeviceInfo();
            
            if (blackholeDeviceInfo != null) {
                tsdMetrics.addProperty("Name", blackholeDeviceInfo.getDeviceName());
                tsdMetrics.addProperty("Enabled", String.valueOf(blackholeDeviceInfo.isEnabled()));
            }

            BlackholeDevice blackholeDevice = null;
            try {
                requestValidator.validateCreateBlackholeDeviceRequest(request);
                assert blackholeDeviceInfo != null; // validateCreateBlackholeDeviceRequest() makes sure it is not null
                blackholeDevice = BlackholeDeviceConverter.convertBlackholeDeviceInfoRequest(blackholeDeviceInfo);
            } catch (IllegalArgumentException ex) {
                String message = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "CreateBlackholeDeviceActivity", ex.getMessage());
                LOG.info(message + " for request: " + ReflectionToStringBuilder.toString(request), ex);
                requestSuccessfullyProcessed = false;
                tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateBlackholeDeviceExceptions.BadRequest.name());
                throw new BadRequest400(message);
            }
            blackholeDeviceInfo.setVersion(blackholeMitigationHelper.updateBlackholeDevice(blackholeDevice, tsdMetrics).getVersion());
            
            CreateBlackholeDeviceResponse response = new CreateBlackholeDeviceResponse();
            response.setRequestId(requestId);
            response.setBlackholeDeviceInfo(blackholeDeviceInfo);
            return response;
        
        } catch (BadRequest400 badrequest) {
            throw badrequest;
        } catch (ConditionalCheckFailedException ex) {
            String message = String.format(ActivityHelper.STALE_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "CreateBlackholeDeviceActivity", ex.getMessage());
            LOG.info(message + " for request: " + ReflectionToStringBuilder.toString(request), ex);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateBlackholeDeviceExceptions.StaleRequest.name());
            throw new StaleRequestException400(message);  
        } catch (Exception internalError) {
            String msg = "Internal error in CreateBlackholeDeviceActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + CreateBlackholeDeviceExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
