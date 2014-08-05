package com.amazon.lookout.mitigation.service.activity;

import java.beans.ConstructorProperties;

import javax.annotation.Nonnull;
import javax.measure.unit.Unit;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.annotation.ThreadSafe;

import com.amazon.coral.annotation.Documentation;
import com.amazon.coral.annotation.Operation;
import com.amazon.coral.annotation.Service;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.service.Activity;
import com.amazon.coral.service.Identity;
import com.amazon.coral.validate.Validated;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.LookoutMitigationServiceConstants;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;

@ThreadSafe
@Service("LookoutMitigationService")
public class EditMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(EditMitigationActivity.class);
    
    private static final String OPERATION_NAME_FOR_AUTH_CHECK = "editMitigation";
    
    @Validated
    @Operation("EditMitigation")
    @Documentation("EditMitigation")
    public @Nonnull MitigationModificationResponse enact(@Nonnull EditMitigationRequest editRequest) {

        Metrics tsdMetrics = getMetrics();
        // This counter is used to calculate the service workload for this operation.
        tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_SUCCESS, 1, Unit.ONE);
        MitigationModificationResponse mitigationModificationResponse = null;
        try {
            LOG.info(String.format("Request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(editRequest)));
            
            String mitigationTemplate = editRequest.getMitigationTemplate();
            DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
            String deviceName = deviceNameAndScope.getDeviceName().name();
            String serviceName = editRequest.getServiceName();
            
            mitigationModificationResponse = new MitigationModificationResponse();            
            mitigationModificationResponse.setMitigationName(editRequest.getMitigationName());
            mitigationModificationResponse.setMitigationTemplate(editRequest.getMitigationTemplate());
            mitigationModificationResponse.setDeviceName(deviceName);
            mitigationModificationResponse.setServiceName(serviceName);
            // mitigationModificationResponse.setRequestStatus(WorkflowStatus.RUNNING);

            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 0, Unit.ONE);
        } catch (BadRequest400 badRequest) {   

            LOG.warn(String.format("Bad request while fulfilling request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(editRequest)), badRequest);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 0, Unit.ONE);
            throw badRequest;
        } catch (Exception internalError) {       

            LOG.error(String.format("Internal error while fulfilling request for EditMitigationActivity: %s.", ReflectionToStringBuilder.toString(editRequest)), internalError);
            tsdMetrics.addCount(LookoutMitigationServiceConstants.ENACT_FAILURE, 1, Unit.ONE);
            throw new InternalServerError500(internalError.getMessage(), internalError);
        } 
        
        return mitigationModificationResponse;
    } 
}
