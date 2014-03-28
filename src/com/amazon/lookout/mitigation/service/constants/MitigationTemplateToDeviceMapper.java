package com.amazon.lookout.mitigation.service.constants;

import javax.annotation.Nullable;

import com.amazon.coral.google.common.collect.ImmutableMap;
import com.amazon.lookout.mitigation.service.mitigations.MitigationTemplate;

/**
 * MitigationTemplateToDeviceMapper maintains a mapping for each template, of which device they correspond to.
 * The device is represented by DeviceNameAndScope wrapper, to represent the deviceName and deviceScope.
 *
 */
public class MitigationTemplateToDeviceMapper {
    private static final ImmutableMap<String, DeviceNameAndScope> templateToDeviceNameAndScopeMapping = 
            new ImmutableMap.Builder<String, DeviceNameAndScope>().put(MitigationTemplate.Router_RateLimit_Route53Customer, new DeviceNameAndScope(DeviceName.POP_ROUTER, DeviceScope.GLOBAL))
                                                                    .build();
    
    // String used by clients in case they fail to find a mapping. Using a standard tag as the prefix allows us to monitor for such cases using logscans.
    public static final String MISSING_DEVICE_MAPPING_KEY = "[MISSING_DEVICE_MAPPING]";
    
    /**
     * Return DeviceNameAndScope for the mitigationTemplate passed as input. Null if this mapping isn't found or if a null template is passed.
     * @param mitigationTemplate MitigationTemplate for which the mapping is requested.
     * @return DeviceNameAndScope corresponding to the template passed as input. Null if no mapping is found (also Null if the mitigationTemplate passed is null).
     */
    public static DeviceNameAndScope getDeviceNameAndScopeForTemplate(@Nullable String mitigationTemplate) {
        return templateToDeviceNameAndScopeMapping.get(mitigationTemplate);
    }

}
