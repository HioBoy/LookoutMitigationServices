package com.amazon.lookout.mitigation.service.activity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.amazon.lookout.ddb.model.TransitProvider;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListTransitProvidersRequest;
import com.amazon.lookout.mitigation.service.ListTransitProvidersResponse;
import com.amazon.lookout.mitigation.service.TransitProviderInfo;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.TransitProviderConverter;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ListTransitProvidersActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListTransitProvidersActivity.class);
    
    private enum ListTransitProvidersExceptions {
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the ActivityHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(ListTransitProvidersExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    @NonNull private final BlackholeMitigationHelper blackholeMitigationHelper;
    
    @Validated
    @Operation("ListTransitProviders")
    @Documentation("ListTransitProviders")
    public @Nonnull ListTransitProvidersResponse enact(@Nonnull ListTransitProvidersRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ListTransitProviders.enact");
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try { 
            LOG.info(String.format("ListTransitProvidersActivity called with RequestId: %s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            List<TransitProvider> transitProviders = blackholeMitigationHelper.listTransitProviders(tsdMetrics);
            List<TransitProviderInfo> transitProvidersInfo = new ArrayList<>(transitProviders.size());
            for (TransitProvider transitProvider : transitProviders) {
                transitProvidersInfo.add(TransitProviderConverter.convertTransitProviderInfoResponse(transitProvider)); 
            }
            
            ListTransitProvidersResponse response = new ListTransitProvidersResponse();
            response.setRequestId(requestId);
            response.setTransitProvidersInfo(transitProvidersInfo);
            
            return response;
        } catch (Exception internalError) {
            String msg = String.format("Internal error in ListTransitProvidersActivity for requestId: %s, reason: %s", requestId, internalError.getMessage());
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListTransitProvidersExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}