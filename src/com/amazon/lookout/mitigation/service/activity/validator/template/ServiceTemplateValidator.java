package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

/**
 * Validator for a request based on the template, with checks specific to the service whom this request belongs to.
 *
 */
public interface ServiceTemplateValidator {
    
    public void validateRequestForTemplate(MitigationModificationRequest request, String mitigationTemplate, TSDMetrics metrics);
    
}
