package com.amazon.lookout.mitigation.service.constants;

import com.amazon.coral.google.common.collect.ImmutableMap;
import com.amazon.lookout.mitigation.service.mitigations.MitigationTemplate;

public class MitigationTemplateToDeviceMapper {
	
	public static final String MISSING_DEVICE_MAPPING_KEY = "[MISSING_DEVICE_MAPPING]";
	
	private static final ImmutableMap<String, DeviceNameAndScope> templateToDeviceNameAndScopeMapping = 
			new ImmutableMap.Builder<String, DeviceNameAndScope>().put(MitigationTemplate.Router_RateLimit_Route53Customer, new DeviceNameAndScope(DeviceName.POP_ROUTER, DeviceScope.GLOBAL))
			                                              		  .build();
	
	public static DeviceNameAndScope getDeviceNameAndScopeForTemplate(String mitigationTemplate) {
		return templateToDeviceNameAndScopeMapping.get(mitigationTemplate);
	}

}
