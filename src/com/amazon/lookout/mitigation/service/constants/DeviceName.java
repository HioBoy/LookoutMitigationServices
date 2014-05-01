package com.amazon.lookout.mitigation.service.constants;

/**
 * This enum lists the different logical devices supported by this service for
 * applying mitigations. These device names are logical since there could
 * possibly be a case where a new device is defined which might be a combination
 * of 2 or more other physical devices.
 * 
 * ANY_DEVICE is a special device which in combination with a ServiceName refers
 * to any device associated with that serviceName. This device is currently only
 * used to authorize read only requests for all mitigations (using any device)
 * for a service.
 */
public enum DeviceName {
    POP_ROUTER, ANY_DEVICE
}