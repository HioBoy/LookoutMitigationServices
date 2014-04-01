package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;

/**
 * Interface defining the responsibilities of a validator which validates requests based on the template and the device for a service whom the request belongs to.
 */
public interface DeviceBasedServiceTemplateValidator extends ServiceTemplateValidator {
    public void validateRequestForTemplateAndDevice(MitigationModificationRequest request, String mitigationTemplate, DeviceNameAndScope deviceNameAndScope, TSDMetrics metrics);
}
