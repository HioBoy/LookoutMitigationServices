package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
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
import com.amazon.lookout.activities.model.MitigationNameAndRequestStatus;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusResponse;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;

@ThreadSafe
@Service("LookoutMitigationService")
public class GetRequestStatusActivity extends Activity{
    private static final Log LOG = LogFactory.getLog(GetRequestStatusActivity.class);
    
    private final RequestInfoHandler requestInfoHandler;
    private final MitigationInstanceInfoHandler mitigationInfoHandler;
    private final RequestValidator requestValidator;
    
    @ConstructorProperties({"requestValidator", "requestInfoHandler", "mitigationInfoHandler"})
    public GetRequestStatusActivity(@Nonnull RequestValidator requestValidator, @Nonnull RequestInfoHandler requestInfoHandler, @Nonnull MitigationInstanceInfoHandler mitigationInfoHandler) {
        Validate.notNull(requestValidator);
        this.requestValidator = requestValidator;
        
        Validate.notNull(requestInfoHandler);
        this.requestInfoHandler = requestInfoHandler;
        
        Validate.notNull(mitigationInfoHandler);
        this.mitigationInfoHandler = mitigationInfoHandler;
        
    }
    
    @Validated
    @Operation("GetRequestStatus")
    @Documentation("GetRequestStatus")
    public @Nonnull GetRequestStatusResponse enact(@Nonnull GetRequestStatusRequest request) {
        // Wrap the CoralMetrics for this activity in a TSDMetrics instance
        TSDMetrics tsdMetrics = new TSDMetrics(getMetrics(), "GetRequestStatus.enact");
        
        String requestId = getRequestId().toString();
        boolean requestSuccessfullyProcessed = true;
        String deviceName = request.getDeviceName();
        String serviceName = request.getServiceName();
        long jobId = Long.valueOf(request.getJobId());
        try {            
            LOG.info(String.format("GetRequestStatusActivity called with RequestId: %s and Request: %s.", requestId, ReflectionToStringBuilder.toString(request)));  
            
            // Step 1. Validate this request
            requestValidator.validateGetRequestStatusRequest(request);
            // Step 2. Get the mitigation name and request-status
            MitigationNameAndRequestStatus mitigationNameAndRequestStatus = requestInfoHandler.getMitigationNameAndRequestStatus(deviceName, jobId, tsdMetrics);
            // Step 3. Get the mitigation instance statuses by location from the MITIGATION_INSTANCES_TABLE
            List<MitigationInstanceStatus> mitigationStatusesResult = mitigationInfoHandler.getMitigationInstanceStatus(deviceName, jobId, tsdMetrics);
            GetRequestStatusResponse response = new GetRequestStatusResponse();
            // Step 4. Set the results from the previous steps in your response object.
            response.setMitigationName(mitigationNameAndRequestStatus.getMitigationName());
            response.setServiceName(serviceName);
            response.setDeviceName(deviceName);
            response.setRequestStatus(mitigationNameAndRequestStatus.getRequestStatus());
            response.setMitigationInstanceStatuses(mitigationStatusesResult);
            
            return response;
        } catch (IllegalArgumentException ex) {
            String msg = String.format("Caught IllegalArgumentException in request for GetRequestStatusActivity for requestId: " + requestId + 
                    ", reason: " + ex.getMessage() + " for request: " + ReflectionToStringBuilder.toString(request));
            LOG.warn(msg, ex);
            throw new BadRequest400(msg, ex);
        } catch (Exception internalError) {
            String msg = String.format("Internal error while fulfilling request for GetRequestStatusActivity for requestId: " + requestId + 
                    " with request: " + ReflectionToStringBuilder.toString(request));
            LOG.error(msg, internalError);
            requestSuccessfullyProcessed = false;
            throw new InternalServerError500(msg);
        } finally {
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, requestSuccessfullyProcessed ? 1 : 0);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, requestSuccessfullyProcessed ? 0 : 1);
            tsdMetrics.end();
        }
    }
}