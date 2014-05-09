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
import com.amazon.lookout.mitigation.service.helpers.AWSUserGroupBasedAuthorizer;

@ThreadSafe
@Service("LookoutMitigationService")
public class EditMitigationActivity extends Activity {
    private static final Log LOG = LogFactory.getLog(EditMitigationActivity.class);
    
    private static final String OPERATION_NAME_FOR_AUTH_CHECK = "editMitigation";
    
    private final AWSUserGroupBasedAuthorizer authorizer;
    
    @ConstructorProperties({"authorizer"})
    public EditMitigationActivity(@Nonnull AWSUserGroupBasedAuthorizer authorizer) {
        Validate.notNull(authorizer);
        this.authorizer = authorizer;
    }

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
            
            // Step1. Authorize this request.
            boolean isAuthorized = authorizer.isClientAuthorized(getIdentity(), serviceName, deviceName, OPERATION_NAME_FOR_AUTH_CHECK);
            if (!isAuthorized) {
                authorizer.setAuthorizedFlag(getIdentity(), false);
                
                MitigationActionMetadata metadata = editRequest.getMitigationActionMetadata();
                String msg = metadata.getUser() + " not authorized to call EditMitigation for service: " + serviceName + 
                             " for device: " + deviceName + " using mitigation template: " + mitigationTemplate + ". Request signed with AccessKeyId: " + 
                             getIdentity().getAttribute(Identity.AWS_ACCESS_KEY) + " and belonging to groups: " + getIdentity().getAttribute(Identity.AWS_USER_GROUPS);
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            } else {
                authorizer.setAuthorizedFlag(getIdentity(), true);
            }
            
            mitigationModificationResponse = new MitigationModificationResponse();            
            mitigationModificationResponse.setMitigationName(editRequest.getMitigationName());            
            mitigationModificationResponse.setServiceName(editRequest.getServiceName());

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
