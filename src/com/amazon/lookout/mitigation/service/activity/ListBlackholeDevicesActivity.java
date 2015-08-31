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
import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.mitigation.service.BlackholeDeviceInfo;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.ListBlackholeDevicesRequest;
import com.amazon.lookout.mitigation.service.ListBlackholeDevicesResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ActivityHelper;
import com.amazon.lookout.mitigation.service.activity.helper.BlackholeDeviceConverter;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;

@AllArgsConstructor
@ThreadSafe
@Service("LookoutMitigationService")
public class ListBlackholeDevicesActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(ListBlackholeDevicesActivity.class);
    
    private enum ListBlackholeDevicesExceptions {
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(
            Arrays.stream(ListBlackholeDevicesExceptions.values())
            .map(e -> e.name())
            .collect(Collectors.toSet()));
    
    @NonNull private final BlackholeMitigationHelper blackholeMitigationHelper;
    
    @Validated
    @Operation("ListBlackholeDevices")
    @Documentation("ListBlackholeDevices")
    public @Nonnull ListBlackholeDevicesResponse enact(@Nonnull ListBlackholeDevicesRequest request) {
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "ListBlackholeDevices.enact");
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        try { 
            LOG.info(String.format("ListBlackholeDevices called with RequestId: %s and request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            ActivityHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            List<BlackholeDevice> blackholeDevices = blackholeMitigationHelper.listBlackholeDevices(tsdMetrics);
            List<BlackholeDeviceInfo> blackholeDevicesInfo = new ArrayList<>(blackholeDevices.size());
            for (BlackholeDevice blackholeDevice : blackholeDevices) {
                blackholeDevicesInfo.add(BlackholeDeviceConverter.convertBlackholeDeviceResponse(blackholeDevice)); 
            }
            
            ListBlackholeDevicesResponse response = new ListBlackholeDevicesResponse();
            response.setRequestId(requestId);
            response.setBlackholeDevicesInfo(blackholeDevicesInfo);
            
            return response;
        } catch (Exception internalError) {
            String msg = String.format("Internal error in ListBlackholeDevicesActivity for requestId: %s, reason: %s", requestId, internalError.getMessage());
            LOG.error(LookoutMitigationServiceConstants.CRITICAL_ACTIVITY_ERROR_LOG_PREFIX + msg + " for request " + ReflectionToStringBuilder.toString(request), internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(ActivityHelper.EXCEPTION_COUNT_METRIC_PREFIX + ListBlackholeDevicesExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}