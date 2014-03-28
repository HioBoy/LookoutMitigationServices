package com.amazon.lookout.mitigation.service.constants;

/**
 * This enum lists the different logical devices supported by this service for applying mitigations.
 * These device names are logical since there could possibly be a case where a new device is defined which might be a combination
 * of 2 or more other physical devices.
 * 
 */
public enum DeviceName {
    POP_ROUTER;
}
