package com.amazon.lookout.mitigation.service.constants;

/**
 * This enum lists the different scopes that the devices supported by this service might operate in.
 * There could possibly be a case where a new scope is defined which might be a combination of 2 or more other scopes (eg: NorthAmerica = {US, CA}).
 * 
 */
public enum DeviceScope {
    GLOBAL;
}
