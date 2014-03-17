package com.amazon.lookout.mitigation.service.activity;

import javax.annotation.Nonnull;
import javax.measure.unit.Unit;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.Activity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.commons.LookoutMitigationServiceConstants;

@ThreadSafe
@Service("LookoutMitigationService")
public class EditMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(EditMitigationActivity.class);

    @Validated
    @Operation("EditMitigation")
    @Documentation("EditMitigation")
    public @Nonnull MitigationModificationResponse enact(@Nonnull MitigationModificationRequest mitigationModificationRequest) {

        Metrics tsdMetrics = getMetrics();
        // This counter is used to calculate the service workload for this operation.
        tsdMetrics.addCount(LookoutMitigationServiceConstants.REQUEST_ENACTED, 1, Unit.ONE);
        MitigationModificationResponse mitigationModificationResponse = null;
        try {
            LOG.info(String.format("Request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(mitigationModificationRequest)));  
            
            mitigationModificationResponse = new MitigationModificationResponse();            
            mitigationModificationResponse.setMitigationName(mitigationModificationRequest.getMitigationName());            
            mitigationModificationResponse.setServiceName(mitigationModificationRequest.getServiceName());

            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 0, Unit.ONE);
        } catch (BadRequest400 badRequest) {   

            LOG.warn(String.format("Bad request while fulfilling request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(mitigationModificationRequest)), badRequest);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 0, Unit.ONE);
            throw badRequest;
        } catch (Exception internalError) {       

            LOG.error(String.format("Internal error while fulfilling request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(mitigationModificationRequest)), internalError);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 1, Unit.ONE);
            throw new InternalServerError500(internalError.getMessage(), internalError);
        } 
        
        return mitigationModificationResponse;
    } 
}
