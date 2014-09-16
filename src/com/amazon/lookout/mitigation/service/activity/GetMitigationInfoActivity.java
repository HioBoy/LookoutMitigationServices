package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

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
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatus;
import com.amazon.lookout.mitigation.service.activity.helper.CommonActivityMetricsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.google.common.collect.Sets;

/**
 * Activity to get details for a particular mitigationName associated with a particular service/device/deviceScope.
 * 
 * Note that when a new mitigation is being updated, there might exist 2 records with the same mitigationName for the
 * same serviceName+deviceName+deviceScope - hence this API returns a list of mitigation descriptions, letting the client 
 * decide which one is the appropriate one for it to consume. 
 *
 */
@Service("LookoutMitigationService")
public class GetMitigationInfoActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(GetMitigationInfoActivity.class);
    
    private enum GetMitigationInfoExceptions {
        BadRequest,
        InternalError
    };
    
    // Maintain a Set<String> for all the exceptions to allow passing it to the CommonActivityMetricsHelper which is called from
    // different activities. Hence not using an EnumSet in this case.
    private static final Set<String> REQUEST_EXCEPTIONS = Collections.unmodifiableSet(Sets.newHashSet(GetMitigationInfoExceptions.BadRequest.name(),
                                                                                                      GetMitigationInfoExceptions.InternalError.name())); 
    
    private final RequestValidator requestValidator;
    private final RequestInfoHandler requestInfoHandler;
    private final MitigationInstanceInfoHandler mitigationInstanceHandler;
    
    @ConstructorProperties({"requestValidator", "requestInfoHandler", "mitigationInstanceHandler"})
    public GetMitigationInfoActivity(@Nonnull RequestValidator requestValidator, @Nonnull RequestInfoHandler requestInfoHandler, 
                                     @Nonnull MitigationInstanceInfoHandler mitigationInstanceHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestInfoHandler);
        this.requestInfoHandler = requestInfoHandler;
        
        Validate.notNull(mitigationInstanceHandler);
        this.mitigationInstanceHandler = mitigationInstanceHandler;
    }

    @Validated
    @Operation("GetMitigationInfo")
    @Documentation("GetMitigationInfo")
    public @Nonnull GetMitigationInfoResponse enact(@Nonnull GetMitigationInfoRequest request) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance.
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetMitigationInfo.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        
        String deviceName = request.getDeviceName();
        String deviceScope = request.getDeviceScope();
        String serviceName = request.getServiceName();
        String mitigationName = request.getMitigationName();
        
        try {            
            LOG.info(String.format("GetMitigationInfoActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));
            CommonActivityMetricsHelper.initializeRequestExceptionCounts(REQUEST_EXCEPTIONS, tsdMetrics);
            
            // Step 1. Validate this request
            requestValidator.validateGetMitigationInfoRequest(request);
            
            List<MitigationRequestDescriptionWithStatus> mitigationDescriptionWithStatuses = new ArrayList<>();
            
            // Step 2. Get list of "active" mitigation requests for this service, device, deviceScope and mitigationName from the requests table.
            List<MitigationRequestDescription> mitigationDescriptions = requestInfoHandler.getMitigationRequestDescriptionsForMitigation(serviceName, deviceName, deviceScope, mitigationName, tsdMetrics);
            
            // Step 3. For each of the requests fetched above, query the individual instance status and populate a new MitigationRequestDescriptionWithStatus instance to wrap this information.
            for (MitigationRequestDescription description : mitigationDescriptions) {
                List<MitigationInstanceStatus> instanceStatuses = mitigationInstanceHandler.getMitigationInstanceStatus(deviceName, description.getJobId(), tsdMetrics);
                
                MitigationRequestDescriptionWithStatus mitigationDescriptionWithStatus = new MitigationRequestDescriptionWithStatus();
                mitigationDescriptionWithStatus.setMitigationRequestDescription(description);
                mitigationDescriptionWithStatus.setInstancesStatus(instanceStatuses);
                mitigationDescriptionWithStatuses.add(mitigationDescriptionWithStatus);
            }
            
            // Step 4. Create the response object to return back to the client.
            GetMitigationInfoResponse response = new GetMitigationInfoResponse();
            response.setDeviceName(deviceName);
            response.setDeviceScope(deviceScope);
            response.setMitigationName(mitigationName);
            response.setServiceName(serviceName);
            response.setMitigationRequestDescriptionsWithStatus(mitigationDescriptionWithStatuses);
            return response;
        } catch (IllegalArgumentException | IllegalStateException ex) {
            String msg = "Caught Exception in request for GetMitigationInfoActivity for requestId: " + requestId + " for request: " + ReflectionToStringBuilder.toString(request);
            LOG.warn(msg, ex);
            tsdMetrics.addCount(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationInfoExceptions.BadRequest.name(), 1);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = "Internal error while fulfilling request for GetRequestStatusActivity for requestId: " + requestId + " with request: " + ReflectionToStringBuilder.toString(request);
            LOG.error(msg, internalError);
            requestSuccessfullyProcessed = false;
            tsdMetrics.addCount(CommonActivityMetricsHelper.EXCEPTION_COUNT_METRIC_PREFIX + GetMitigationInfoExceptions.InternalError.name(), 1);
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}
