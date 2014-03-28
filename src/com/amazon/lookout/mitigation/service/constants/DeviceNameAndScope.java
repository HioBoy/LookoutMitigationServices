package com.amazon.lookout.mitigation.service.constants;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A convenience wrapper for DeviceName and DeviceScope.
 */
@Immutable
public class DeviceNameAndScope {
    
    private final DeviceName deviceName;
    private final DeviceScope deviceScope;
    
    public DeviceNameAndScope(@Nullable DeviceName deviceName, @Nullable DeviceScope deviceScope) {
        this.deviceName = deviceName;
        this.deviceScope = deviceScope;
    }
    
    public DeviceName getDeviceName() {
        return deviceName;
    }

    public DeviceScope getDeviceScope() {
        return deviceScope;
    }
}
