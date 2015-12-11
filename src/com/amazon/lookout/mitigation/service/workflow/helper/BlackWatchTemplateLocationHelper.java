package com.amazon.lookout.mitigation.service.workflow.helper;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import lombok.NonNull;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MissingMitigationException400;
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
        
        String deviceName = deviceNameAndScope.getDeviceName().name();
        String deviceScope = deviceNameAndScope.getDeviceScope().name();
        List<MitigationRequestDescriptionWithLocations> mitigationDescriptions =
                requestInfoHandler.getMitigationHistoryForMitigation(request.getServiceName(), 
                        deviceName, deviceScope, request.getMitigationName(), null, 1, tsdMetrics);
        if (mitigationDescriptions.isEmpty()) {
            throw new MissingMitigationException400(
                    "No active mitigation to delete found when querying for deviceName: " + deviceName + " deviceScope: " + deviceScope + 
                    ", for request: " + ReflectionToStringBuilder.toString(request));
        }
        return new HashSet<>(mitigationDescriptions.get(0).getLocations());
    }
}
