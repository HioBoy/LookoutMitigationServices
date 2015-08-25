package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

/**
 * Packet attribute of Traffic filter.
 * All packet attribute should implement this interface
 * @author xingbow
 */
public interface PacketAttribute {
    /**
     * Find overlap packet attribute value between current packet attribute and another packet attribute.
     * @param o : another packet attribute object
     * @return : overlap packet attribute object. Null if not found
     */
    public PacketAttribute findOverlap(PacketAttribute o);
}
