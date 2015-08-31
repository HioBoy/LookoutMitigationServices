package com.amazon.lookout.mitigation.service.activity.helper;

import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.mitigation.service.BlackholeDeviceInfo;

public class BlackholeDeviceConverter {
    
    public static BlackholeDevice convertBlackholeDeviceInfoRequest(BlackholeDeviceInfo blackholeDeviceInfo) {
        BlackholeDevice blackholeDevice = new BlackholeDevice();
        blackholeDevice.setDeviceName(blackholeDeviceInfo.getDeviceName());
        blackholeDevice.setDeviceDescription(blackholeDeviceInfo.getDeviceDescription());
        blackholeDevice.setVersion(blackholeDeviceInfo.getVersion());
        return blackholeDevice;
    }
    
    public static BlackholeDeviceInfo convertBlackholeDeviceResponse(BlackholeDevice blackholeDevice) {
        BlackholeDeviceInfo blackholeDeviceInfo = new BlackholeDeviceInfo();
        blackholeDeviceInfo.setDeviceName(blackholeDevice.getDeviceName());
        blackholeDeviceInfo.setDeviceDescription(blackholeDevice.getDeviceDescription());
        blackholeDeviceInfo.setVersion(blackholeDevice.getVersion());
        return blackholeDeviceInfo;
    }
}
