package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;
import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

/**
 * Helper to perform operations which are common across all activities.
 */
@ThreadSafe
public class ActivityHelper {
    
    private static final String TEMPLATE_NAME_COUNT_METRIC_PREFIX = "TemplateName:";
    private static final String DEVICE_NAME_COUNT_METRIC_PREFIX = "DeviceName:";
    private static final String DEVICE_SCOPE_COUNT_METRIC_PREFIX = "DeviceScope:";
    private static final String SERVICE_NAME_COUNT_METRIC_PREFIX = "ServiceName:";
    
    // Declared as public to allow individual Activities to prefix the exceptions they receive with this.
    public static final String EXCEPTION_COUNT_METRIC_PREFIX = "Exception:";
    
    public static final String BAD_REQUEST_EXCEPTION_MESSAGE_FORMAT = "Received BadRequest for requestId: %s in activity: %s. Detailed message: %s";
    public static final String STALE_REQUEST_EXCEPTION_MESSAGE_FORMAT = "Received StaleRequest for requestId: %s in activity: %s. Detailed message: %s";
    
    /**
     * Add metrics to be able to track the template for which a request was made.
     * @param templateInRequest String representing the template that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addTemplateNameCountMetrics(@NonNull String templateInRequest, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(templateInRequest);
        
        // Add the metric to track the template for which this request was made.
        for (String template : MitigationTemplate.values()) {
            tsdMetrics.addCount(TEMPLATE_NAME_COUNT_METRIC_PREFIX + template, 0);
        }
        tsdMetrics.addCount(TEMPLATE_NAME_COUNT_METRIC_PREFIX + templateInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the device for which a request was made.
     * @param deviceNameInRequest String representing the deviceName that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addDeviceNameCountMetrics(@NonNull String deviceNameInRequest, @NonNull TSDMetrics tsdMetrics) {
        // Add the metric to track the device for which this request was made.
        for (DeviceName device : DeviceName.values()) {
            tsdMetrics.addCount(DEVICE_NAME_COUNT_METRIC_PREFIX + device.name(), 0);
        }
        tsdMetrics.addCount(DEVICE_NAME_COUNT_METRIC_PREFIX + deviceNameInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the deviceScope for which a request was made.
     * @param deviceScopeInRequest String representing the deviceScope that was passed in the request.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addDeviceScopeCountMetrics(@NonNull String deviceScopeInRequest, @NonNull TSDMetrics tsdMetrics) {
        // Add the metric to track the device for which this request was made.
        for (DeviceScope deviceScope : DeviceScope.values()) {
            tsdMetrics.addCount(DEVICE_SCOPE_COUNT_METRIC_PREFIX + deviceScope.name(), 0);
        }
        tsdMetrics.addCount(DEVICE_SCOPE_COUNT_METRIC_PREFIX + deviceScopeInRequest, 1);
    }
    
    /**
     * Add metrics to be able to track the service for which a request was made.
     * @param serviceNameInRequest String representing the serviceName for which the request was made.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void addServiceNameCountMetrics(@NonNull String serviceNameInRequest, @NonNull TSDMetrics tsdMetrics) {
        // Add the metric to track the device for which this request was made.
        for (String serviceName : ServiceName.values()) {
            tsdMetrics.addCount(SERVICE_NAME_COUNT_METRIC_PREFIX + serviceName, 0);
        }
        tsdMetrics.addCount(SERVICE_NAME_COUNT_METRIC_PREFIX + serviceNameInRequest, 1);
    }
    
    /**
     * Helper to initialize the different exceptions thrown by an activity to 0 count value.
     * @param exceptions Set of String representing the exceptions thrown by an activity.
     * @param tsdMetrics TSDMetrics instance to which we need to add the count metrics.
     */
    public static void initializeRequestExceptionCounts(@NonNull Set<String> exceptions, @NonNull TSDMetrics tsdMetrics) {
        Validate.notEmpty(exceptions);
        
        // Initialize all requestExceptions to 0.
        for (String exception : exceptions) {
            tsdMetrics.addCount(EXCEPTION_COUNT_METRIC_PREFIX + exception, 0);
        }
    }
}
