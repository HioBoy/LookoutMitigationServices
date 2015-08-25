package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import java.util.EnumMap;
import java.util.Map;

import org.junit.Test;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.TrafficFilter.AttributeName;
import com.google.common.collect.Sets;

public class TrafficFilterTest {
    /**
     * Test attribute name does not match attribute
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_1() {
        // validate attribute name and attribute must match each other
        Map<AttributeName, PacketAttribute> attributes = new EnumMap<>(AttributeName.class);
        attributes.put(AttributeName.SOURCE_PORT, NetworkCidr.fromString("1.1.1.1/32"));
        TrafficFilter.createFilter(attributes, 1);
    }

    /**
     * Test attribute name does not match attribute
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_2() {
        // validate attribute name and attribute must match each other
        Map<AttributeName, PacketAttribute> attributes = new EnumMap<>(AttributeName.class);
        attributes.put(AttributeName.SOURCE_PORT, NetworkCidr.fromString("1.1.1.1/32"));
        TrafficFilter.createFilter(attributes, Sets.newHashSet(1));
    }
    
    /**
     * Test overlap. filter A contains IP, filter B contains port range. Both of them contains l4 protocol
     */
    @Test
    public void testOverlap_1() {
        Map<AttributeName, PacketAttribute> attributesA = new EnumMap<>(AttributeName.class);
        attributesA.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesA.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20000"));
        attributesA.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterA = TrafficFilter.createFilter(attributesA, Sets.newHashSet(1));
        
        Map<AttributeName, PacketAttribute> attributesB = new EnumMap<>(AttributeName.class);
        attributesB.put(AttributeName.DEST_PORT, PortRange.fromString("1000:10000"));
        attributesB.put(AttributeName.SOURCE_PORT, PortRange.fromString("1000:10000"));
        attributesB.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterB = TrafficFilter.createFilter(attributesB, Sets.newHashSet(2));
        
        TrafficFilter overlapFilter = filterA.findOverlap(filterB);
        
        assertEquals(attributesA.get(AttributeName.SOURCE_IP), overlapFilter.getAttributes().get(AttributeName.SOURCE_IP));
        assertEquals(attributesB.get(AttributeName.DEST_PORT), overlapFilter.getAttributes().get(AttributeName.DEST_PORT));
        assertEquals(PortRange.fromString("2000:10000"), overlapFilter.getAttributes().get(AttributeName.SOURCE_PORT));
        assertEquals(attributesA.get(AttributeName.L4_PROTOCOL), overlapFilter.getAttributes().get(AttributeName.L4_PROTOCOL));
        assertEquals(null, overlapFilter.getAttributes().get(AttributeName.DEST_IP));
        assertEquals(Sets.newHashSet(1,2), overlapFilter.getRelatedOriginFilterIndices());
    }
        
    /**
     * overlap not exist
     */
    @Test
    public void testOverlap_2() {
        Map<AttributeName, PacketAttribute> attributesA = new EnumMap<>(AttributeName.class);
        attributesA.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesA.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20000"));
        attributesA.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterA = TrafficFilter.createFilter(attributesA, Sets.newHashSet(1));
        
        Map<AttributeName, PacketAttribute> attributesB = new EnumMap<>(AttributeName.class);
        attributesB.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.2/32"));
        attributesB.put(AttributeName.DEST_PORT, PortRange.fromString("1000:10000"));
        attributesB.put(AttributeName.SOURCE_PORT, PortRange.fromString("1000:10000"));
        attributesB.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterB = TrafficFilter.createFilter(attributesB, Sets.newHashSet(2));
        
        assertNull(filterA.findOverlap(filterB));
    }
    
    @Test
    public void testSameDefAs() {
        Map<AttributeName, PacketAttribute> attributesA = new EnumMap<>(AttributeName.class);
        attributesA.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesA.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20000"));
        attributesA.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterA = TrafficFilter.createFilter(attributesA, Sets.newHashSet(1));

        Map<AttributeName, PacketAttribute> attributesB = new EnumMap<>(AttributeName.class);
        attributesB.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesB.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20000"));
        attributesB.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterB = TrafficFilter.createFilter(attributesB, Sets.newHashSet(2));
         
        assertTrue(filterA.sameDefAs(filterB));
    }
    
    @Test
    public void testSameDefAs2() {
        Map<AttributeName, PacketAttribute> attributesA = new EnumMap<>(AttributeName.class);
        attributesA.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesA.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20000"));
        attributesA.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterA = TrafficFilter.createFilter(attributesA, Sets.newHashSet(1));

        Map<AttributeName, PacketAttribute> attributesB = new EnumMap<>(AttributeName.class);
        attributesB.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        attributesB.put(AttributeName.SOURCE_PORT, PortRange.fromString("2000:20001"));
        attributesB.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString("TCP"));
        TrafficFilter filterB = TrafficFilter.createFilter(attributesB, Sets.newHashSet(1));
         
        assertTrue(!filterA.sameDefAs(filterB));
    }
}
