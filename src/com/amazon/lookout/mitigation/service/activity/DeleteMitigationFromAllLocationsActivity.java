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
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.commons.LookoutMitigationServiceConstants;

@ThreadSafe
@Service("LookoutMitigationService")
public class DeleteMitigationFromAllLocationsActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(DeleteMitigationFromAllLocationsActivity.class);

    @Validated
    @Operation("DeleteMitigationFromAllLocations")
    @Documentation("DeleteMitigationFromAllLocations")
    public @Nonnull MitigationModificationResponse enact(@Nonnull DeleteMitigationFromAllLocationsRequest deleteMitigationFromAllLocationsRequest) {

        Metrics tsdMetrics = getMetrics();
        // This counter is used to calculate the service workload for this operation.
        tsdMetrics.addCount(LookoutMitigationServiceConstants.REQUEST_ENACTED, 1, Unit.ONE);
        MitigationModificationResponse mitigationModificationResponse = null;
        try {            
            LOG.info(String.format("Request for DeleteMitigationFromAllLocationsActivity: %s.", ReflectionToStringBuilder.toString(deleteMitigationFromAllLocationsRequest)));  
            
            mitigationModificationResponse = new MitigationModificationResponse();            
            mitigationModificationResponse.setMitigationName(deleteMitigationFromAllLocationsRequest.getMitigationName());            
            mitigationModificationResponse.setServiceName(deleteMitigationFromAllLocationsRequest.getServiceName());
            
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 0, Unit.ONE);
        } catch (BadRequest400 badRequest) {   

            LOG.warn(String.format("Bad request while fulfilling request for DeleteMitigationFromAllLocationsActivity: %s.", ReflectionToStringBuilder.toString(deleteMitigationFromAllLocationsRequest)), badRequest);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 0, Unit.ONE);
            throw badRequest;
        } catch (Exception internalError) {       

            LOG.error(String.format("Internal error while fulfilling request for DeleteMitigationFromAllLocationsActivity: %s.", ReflectionToStringBuilder.toString(deleteMitigationFromAllLocationsRequest)), internalError);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.INTERNAL_FAILURE, 1, Unit.ONE);
            throw new InternalServerError500(internalError.getMessage(), internalError);
        }      
        
        return mitigationModificationResponse;
    } 
}
