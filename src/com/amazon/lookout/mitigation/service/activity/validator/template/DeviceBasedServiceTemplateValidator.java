package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;

/**
 * Interface defining the responsibilities of a validator which validates requests based on the template and the device for a service whom the request belongs to.
 */
public interface DeviceBasedServiceTemplateValidator extends ServiceTemplateValidator {
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate, DeviceName deviceName,
            TSDMetrics metrics);
    
}

