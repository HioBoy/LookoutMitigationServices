package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.NonNull;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;

public class BlackWatchTemplateLocationHelper implements TemplateBasedLocationsHelper {
    private final RequestInfoHandler requestInfoHandler;
    
    @ConstructorProperties({"requestInfoHandler"})
    public BlackWatchTemplateLocationHelper(@NonNull RequestInfoHandler requestInfoHandler) {
        this.requestInfoHandler = requestInfoHandler;
    }
    
    /**
     * try to get location from request first. Then fallback to query dynamodb
     * to get the latest mitigation deployment locations of this mitigation
     * @throws MissingMitigationException400 if mitigation is not found
     */
    @Override
    public Set<String> getLocationsForDeployment(MitigationModificationRequest request, TSDMetrics tsdMetrics) {
        Set<String> locationsInRequest = TemplateBasedLocationsManager.getLocationsFromRequest(request);
        if (locationsInRequest != null) {
            return locationsInRequest;
        }
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper
                .getDeviceNameAndScopeForTemplate(request.getMitigationTemplate());
        
        List<MitigationRequestDescriptionWithLocations> mitigationDescriptions =
                requestInfoHandler.getMitigationHistoryForMitigation(request.getServiceName(),
                        deviceNameAndScope.getDeviceName().name(), deviceNameAndScope.getDeviceScope().name(),
                        request.getMitigationName(), null, 1, tsdMetrics);
        return new HashSet<>(mitigationDescriptions.get(0).getLocations());
    }
}
