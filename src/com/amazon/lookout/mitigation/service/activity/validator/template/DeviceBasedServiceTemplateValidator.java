package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;

/**
 * Interface defining the responsibilities of a validator which validates requests based on the template and the device for a service whom the request belongs to.
 */
public interface DeviceBasedServiceTemplateValidator extends ServiceTemplateValidator {
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate, DeviceNameAndScope deviceNameAndScope,
            TSDMetrics metrics);
    
    /**
     * deployment check type include pre-deployment check, and post-deployment check
     */
    public enum DeploymentCheckType {PRE, POST};
    
    /**
     * Validate empty deployment checks
     * @param deploymentChecks : list of deployment check
     * @param template : mitigation template
     * @param isPreDeploymentCheck : is pre-deployment check or not. If it is not pre deployment check, then assume it
     *                                  is post-deployment check
     */
    default void validateNoDeploymentChecks(List<MitigationDeploymentCheck> deploymentChecks, String template,
            DeploymentCheckType deploymentCheckType) {
        Validate.notEmpty(template);
        Validate.notNull(deploymentCheckType);
        
        if ((deploymentChecks != null) && !deploymentChecks.isEmpty()) {
            throw new IllegalArgumentException("For MitigationTemplate: " + template + ", we expect to not have "
                    + deploymentCheckType.name().toLowerCase() + "-deployment checks to be performed.");
        }
    }
}
