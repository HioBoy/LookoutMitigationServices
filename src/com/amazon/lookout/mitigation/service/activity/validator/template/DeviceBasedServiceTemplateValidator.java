package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;

public interface DeviceBasedServiceTemplateValidator extends ServiceTemplateValidator {
    
    public void validateRequestForTemplateAndDevice(MitigationModificationRequest request, String mitigationTemplate, DeviceNameAndScope deviceNameAndScope, TSDMetrics metrics);
}
