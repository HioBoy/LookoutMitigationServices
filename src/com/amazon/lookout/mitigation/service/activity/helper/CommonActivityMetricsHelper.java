package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

/**
 * Helper to add metrics which are common across all activities.
 */
@ThreadSafe
public class CommonActivityMetricsHelper {
    
    /**
     * Add metrics to be able to track the template for which a request was made.
     * @param templateInRequest String representing the template that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addTemplateNameCountMetrics(@Nonnull String templateInRequest, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notEmpty(templateInRequest);
        Validate.notNull(tsdMetrics);
        
        // Add the metric to track the template for which this request was made.
        for (String template : MitigationTemplate.values()) {
            tsdMetrics.addCount(template, 0);
        }
        tsdMetrics.addCount(templateInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the device for which a request was made.
     * @param deviceNameInRequest String representing the deviceName that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addDeviceNameCountMetrics(@Nonnull String deviceNameInRequest, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notNull(deviceNameInRequest);
        Validate.notNull(tsdMetrics);
        
        // Add the metric to track the device for which this request was made.
        for (DeviceName device : DeviceName.values()) {
            tsdMetrics.addCount(device.name(), 0);
        }
        tsdMetrics.addCount(deviceNameInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the deviceScope for which a request was made.
     * @param deviceScopeInRequest String representing the deviceScope that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addDeviceScopeCountMetrics(@Nonnull String deviceScopeInRequest, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notNull(deviceScopeInRequest);
        Validate.notNull(tsdMetrics);
        
        // Add the metric to track the device for which this request was made.
        for (DeviceScope deviceScope : DeviceScope.values()) {
            tsdMetrics.addCount(deviceScope.name(), 0);
        }
        tsdMetrics.addCount(deviceScopeInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the service for which a request was made.
     * @param serviceNameInRequest String representing the serviceName for which the request was made.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addServiceNameCountMetrics(@Nonnull String serviceNameInRequest, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notNull(serviceNameInRequest);
        Validate.notNull(tsdMetrics);
        
        // Add the metric to track the device for which this request was made.
        for (String serviceName : ServiceName.values()) {
            tsdMetrics.addCount(serviceName, 0);
        }
        tsdMetrics.addCount(serviceNameInRequest, 1);
    }
    
    /**
     * Helper to initialize the different exceptions thrown by an activity to 0 count value.
     * @param requestStates Set of String representing the exceptions thrown by an activity.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void initializeRequestExceptionCounts(@Nonnull Set<String> exceptions, @Nonnull TSDMetrics tsdMetrics) {
        Validate.notEmpty(exceptions);
        Validate.notNull(tsdMetrics);
        
        // Initialize all requestExceptions to 0.
        for (String exception : exceptions) {
            tsdMetrics.addCount(exception, 0);
        }
    }
}
