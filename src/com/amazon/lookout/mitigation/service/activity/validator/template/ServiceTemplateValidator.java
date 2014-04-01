package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

public interface ServiceTemplateValidator {
	
	public void validateRequestForTemplate(MitigationModificationRequest request, String mitigationTemplate, TSDMetrics metrics);
	
	public void validateCoexistenceForTemplateAndDevice(String templateForNewDefinition, String mitigationNameForNewDefinition, 
			                                            MitigationDefinition newDefinition, String templateForExistingDefinition, 
			                                            String mitigationNameForExistingDefinition, MitigationDefinition existingDefinition, TSDMetrics metrics);
	
	public String getServiceNameToValidate();
}
