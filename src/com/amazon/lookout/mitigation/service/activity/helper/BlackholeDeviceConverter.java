package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.HashSet;
import java.util.stream.Collectors;

import com.amazon.lookout.ddb.model.BlackholeDevice;
import com.amazon.lookout.mitigation.service.BlackholeDeviceInfo;

public class BlackholeDeviceConverter {
    
    public static BlackholeDevice convertBlackholeDeviceInfoRequest(BlackholeDeviceInfo blackholeDeviceInfo) {
        BlackholeDevice blackholeDevice = new BlackholeDevice();
        blackholeDevice.setDeviceName(blackholeDeviceInfo.getDeviceName());
        blackholeDevice.setDeviceDescription(blackholeDeviceInfo.getDeviceDescription());
        blackholeDevice.setEnabled(blackholeDeviceInfo.isEnabled());
        blackholeDevice.setSupportedASNs(new HashSet<Integer>(blackholeDeviceInfo.getSupportedASNs()));
        blackholeDevice.setVersion(blackholeDeviceInfo.getVersion());
        return blackholeDevice;
    }
    
    public static BlackholeDeviceInfo convertBlackholeDeviceResponse(BlackholeDevice blackholeDevice) {
        BlackholeDeviceInfo blackholeDeviceInfo = new BlackholeDeviceInfo();
        blackholeDeviceInfo.setDeviceName(blackholeDevice.getDeviceName());
        blackholeDeviceInfo.setDeviceDescription(blackholeDevice.getDeviceDescription());
        blackholeDeviceInfo.setEnabled(blackholeDevice.isEnabled());
        
        // The sorting isn't needed but it does make the response more consistent
        blackholeDeviceInfo.setSupportedASNs(
                blackholeDevice.getSupportedASNs().stream().sorted().collect(Collectors.toList()));
        
        blackholeDeviceInfo.setVersion(blackholeDevice.getVersion());
        
        return blackholeDeviceInfo;
    }
}
