package com.amazon.lookout.mitigation.service.constants;

import javax.annotation.concurrent.Immutable;

@Immutable
public class DeviceNameAndScope {
	
	private final DeviceName deviceName;
	private final DeviceScope deviceScope;
	
	public DeviceNameAndScope(DeviceName deviceName, DeviceScope deviceScope) {
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
