package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Set;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

public interface TemplateBasedLocationsHelper {
	
	public Set<String> getLocationsForDeployment(MitigationModificationRequest request, TSDMetrics tsdMetrics);

}
