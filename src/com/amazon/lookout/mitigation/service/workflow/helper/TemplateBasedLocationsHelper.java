package com.amazon.lookout.mitigation.service.workflow.helper;

import java.util.Set;

import com.amazon.lookout.mitigation.service.MitigationModificationRequest;

public interface TemplateBasedLocationsHelper {
	
	public Set<String> getLocationsForDeployment(MitigationModificationRequest request);
}
