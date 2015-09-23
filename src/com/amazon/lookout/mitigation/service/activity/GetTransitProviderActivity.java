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
import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.service.Activity;
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetTransitProviderRequest;
import com.amazon.lookout.mitigation.service.GetTransitProviderResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.TransitProviderConverter;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;

@Service("LookoutMitigationService")
public class GetTransitProviderActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetTransitProviderActivity.class);
    
    private enum GetTransitProviderActivityExceptions {
        BadRequest,
        StaleRequest,
        InternalError
    };

    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(GetTransitProviderActivityExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));

    @NonNull private final RequestValidator requestValidator;
    @NonNull private final BlackholeMitigationHelper blackholeMitigationHelper;

    @ConstructorProperties({"requestValidator", "blackholeMitigationHelper"})
    public GetTransitProviderActivity(@Nonnull RequestValidator requestValidator, 
            @Nonnull BlackholeMitigationHelper blackholeMitigationHelper) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(blackholeMitigationHelper);
        this.blackholeMitigationHelper = blackholeMitigationHelper;
    }
    
    @Operation("GetTransitProvider")
    @Documentation("API for getting a transit provider by id")
    public GetTransitProviderResponse enact(GetTransitProviderRequest request)
            throws InternalServerError500,BadRequest400
    {
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "UpdateTransitProvider.enact");
        try { 
            LOG.info(String.format("GetTransitProviderActivity called with RequestId: %s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            tsdMetrics.addProperty("Id", request.getId());
            
            try {
                requestValidator.validateGetTransitProviderRequest(request);
            } catch (IllegalArgumentException ex) {
                String message = String.format(ActivityHelper.BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT, requestId, "GetTransitProviderRequest", ex.getMessage());
                throw new BadRequest400(message);
            }
            
            Optional<TransitProvider> transitProvider = blackholeMitigationHelper.loadTransitProvider(request.getId(), tsdMetrics);
            if (!transitProvider.isPresent()) {
                throw new BadRequest400("Transit provider " + request.getId() + " does not exist");
            }
            
            GetTransitProviderResponse response = new GetTransitProviderResponse();
            response.setRequestId(getRequestId().toString());
            response.setTransitProvider(TransitProviderConverter.convertTransitProviderInfoResponse(transitProvider.get()));
            return response;
        } catch (BadRequest400 badrequest) {
            LOG.info(badrequest.getMessage() + " for request: " + ReflectionToStringBuilder.toString(request), badrequest.getCause());
            tsdMetrics.addOne(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetTransitProviderActivityExceptions.BadRequest.name());
            requestSuccessfullyProcessed = false;
            throw badrequest;
        } catch (Exception internalError) {
            String msg = "Internal error in UpdateTransitProviderActivity for requestId: " + requestId + ", reason: " + internalError.getMessage();
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetTransitProviderActivityExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
