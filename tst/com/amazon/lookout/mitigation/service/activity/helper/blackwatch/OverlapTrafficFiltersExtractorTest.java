package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.TrafficFilter.AttributeName;
import com.google.common.collect.Sets;

/**
 * 1. A,B,C   overlap each other.
 * 2. A,B,C   does not have any overlap.
 * 3. A,B,C   A,B overlap each other.
 * 4. A,B,C   A is inside B, C is overlap A,B
 * 5. A,B,C   A is inside B, B is inside C.
 * 6. A,B,C,D CD overlap with AB
 * 7. A,B,C,D A,B,C overlap each other. D equal AB (D = AB)
 * 8. A,B,C,D A,B,C overlap each other. A equal CD (A = CD)
 * 9 A,B,C,D A,B,C overlap each other. D inside AB(ABD = D), D does not inside ABC
 * 10 A,B,C,D A,B,C overlap each other. D inside AB(ABD = D), D inside ABC
 * 11. A,B,C,D A,B,C overlap each other. AB inside D(ABD = AB)
 * 12. A,B,C,D A,B,C overlap each other, C inside D
 */
public class OverlapTrafficFiltersExtractorTest {
    private TrafficFilter createFilter(String sourceCidrStr, String destCidrStr, String sourcePortRangeStr, String destPortRangeStr, String l4Protocol,
            Set<Integer> relatedOriginFilterIndices) {
        Map<AttributeName, PacketAttribute> attributes = new EnumMap<>(AttributeName.class);
        attributes.put(AttributeName.SOURCE_IP, NetworkCidr.fromString(sourceCidrStr));
        attributes.put(AttributeName.DEST_IP, NetworkCidr.fromString(destCidrStr));
        attributes.put(AttributeName.SOURCE_PORT, PortRange.fromString(sourcePortRangeStr));
        attributes.put(AttributeName.DEST_PORT, PortRange.fromString(destPortRangeStr));
        attributes.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString(l4Protocol));
        return TrafficFilter.createFilter(attributes, relatedOriginFilterIndices);
    }
    
    private Map<AttributeName, PacketAttribute> create5TupleAttributes(String sourceCidrStr, String destCidrStr, String sourcePortRangeStr, String destPortRangeStr, String l4Protocol) {
        Map<AttributeName, PacketAttribute> attributes = new EnumMap<>(AttributeName.class);
        attributes.put(AttributeName.SOURCE_IP, NetworkCidr.fromString(sourceCidrStr));
        attributes.put(AttributeName.DEST_IP, NetworkCidr.fromString(destCidrStr));
        attributes.put(AttributeName.SOURCE_PORT, PortRange.fromString(sourcePortRangeStr));
        attributes.put(AttributeName.DEST_PORT, PortRange.fromString(destPortRangeStr));
        attributes.put(AttributeName.L4_PROTOCOL, L4Protocol.fromString(l4Protocol));
        return attributes;
    }
    
    private void testRunner(List<Map<AttributeName, PacketAttribute>> inputFilters, List<TrafficFilter> expectedFilters) {
        List<TrafficFilter> extractedFilters = OverlapTrafficFiltersExtractor.extractOverlapFilters(inputFilters);
        try {
            assertEquals(String.format("Expect %d number of filters, but found %d", expectedFilters.size(), extractedFilters.size()) , expectedFilters.size(), extractedFilters.size());
            for (int i = 0; i < expectedFilters.size(); i++) {
                assertEquals("filter index : " + i, expectedFilters.get(i), extractedFilters.get(i));
            }
        } catch (Throwable ex) {
            System.out.println("Expected filters:");
            for (int i = 0; i < expectedFilters.size(); i++) {
                System.out.println("filter index : " + i + expectedFilters.get(i));
            }
            System.out.println("Extracted filters:");
            for (int i = 0; i < extractedFilters.size(); i++) {
                System.out.println("filter index : " + i + extractedFilters.get(i));
            }
            throw ex;
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_2() {
        // test duplicate filter exception
        Map<AttributeName, PacketAttribute> attributes = new EnumMap<>(AttributeName.class);
        attributes.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
        Map<AttributeName, PacketAttribute> attributes2 = new EnumMap<>(AttributeName.class);
        attributes2.put(AttributeName.SOURCE_IP, NetworkCidr.fromString("1.1.1.1/32"));
 
        OverlapTrafficFiltersExtractor.extractOverlapFilters(Arrays.asList(attributes, attributes2));
    }

    /**
     * Filter A,B,C   they all overlap each other.
     */
    @Test
    public void test3FilterOverlapEachOther() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * Filter A,B,C   does not have any overlap.
     */
    @Test
    public void test3FilterNoOverlapEachOther() {
         testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("11.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("12.0.0.0/8",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("11.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("12.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A,B,C   A,B overlap each other.
     */
    @Test
    public void test3Filter_2OverlapEachOther() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("11.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("11.0.0.0/8",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("11.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("11.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("11.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A,B,C   A is inside B, C is overlap A,B
     */
    @Test
    public void test3Filters_1() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/16", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/16", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2)),
                        createFilter("10.0.0.0/16", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A is inside B, B is inside C.
     */
    @Test
    public void test3Filters_2() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000",  "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:20000", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0",    "0:65535", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000",  "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:20000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0",    "0:65535", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A,B,C,D    CD overlap with AB
     */
    @Test
    public void test4Filters_1() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8",  "0.0.0.0/0", "1000:12000", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "10.0.0.0/8",  "100:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/8",  "0.0.0.0/0", "1000:22000", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "10.0.0.0/8",    "0:2000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000",  "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0",  "1000:12000", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "100:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 3)),
                        createFilter("10.0.0.0/8",  "0.0.0.0/0", "1000:22000", "0:65535", "17/0xff", Sets.newHashSet(2)),
                        createFilter("0.0.0.0/0",  "10.0.0.0/8",    "0:2000", "0:65535", "17/0xff", Sets.newHashSet(3)))
                );
    }
    
    /**
     * A,B,C,D A,B,C overlap each other. D equal AB (D = AB)
     */
    @Test
    public void test4Fitlers_2() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/8",  "10.0.0.0/8", "0:65535", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A,B,C,D A,B,C overlap each other. A equal CD (A = CD)
     */
    @Test
    public void test4Filter_3() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8",  "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 2, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 3)),
                        createFilter("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(3)))
                );
    }
    
    /**
     * A,B,C,D A,B,C overlap each other. D inside AB(ABD = D), D does not inside ABC
     */
    @Test
    public void test4Filter_4() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/8", "10.0.0.0/8", "60000:65535", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "60000:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
   
    /**
     * A,B,C,D A,B,C overlap each other. D inside AB(ABD = D), D inside ABC
     */
    @Test
    public void test4Filter_5() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/16", "10.0.0.0/16", "1100:1900", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/16", "10.0.0.0/16", "1100:1900", "0:65535", "17/0xff", Sets.newHashSet(0, 2, 1, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)))
                );
    }
    
    /**
     * A,B,C,D   A,B,C overlap each other. AB inside D(ABD = AB)
     */
    @Test
    public void test4Filter_6() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "1000:1535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "1000:1535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("10.0.0.0/7", "10.0.0.0/7", "0:1800", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:1535", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/7", "1000:1535", "0:65535", "17/0xff", Sets.newHashSet(0, 2, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:1535", "0:65535", "17/0xff", Sets.newHashSet(0, 2)),
                        createFilter("10.0.0.0/7", "10.0.0.0/8", "1000:1535", "0:65535", "17/0xff", Sets.newHashSet(1, 2, 3)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:1535", "0:65535", "17/0xff", Sets.newHashSet(1, 2)),
                        createFilter("10.0.0.0/7", "10.0.0.0/7", "1000:1800", "0:65535", "17/0xff", Sets.newHashSet(2, 3)),
                        createFilter("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2)),
                        createFilter("10.0.0.0/7", "10.0.0.0/7", "0:1800", "0:65535", "17/0xff", Sets.newHashSet(3)))
                );
    }

    /**
     * A,B,C,D     A,B,C overlap each other, C inside D
     */
    @Test
    public void test4Filter_7() {
        testRunner(
                Arrays.asList(
                        create5TupleAttributes("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff"),
                        create5TupleAttributes("0.0.0.0/0",  "0.0.0.0/0", "0:20000", "0:65535", "17/0xff")),
                Arrays.asList(
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 2, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:20000", "0:65535", "17/0xff", Sets.newHashSet(0, 1, 3)),
                        createFilter("10.0.0.0/8", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0, 1)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(0, 2, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:20000", "0:65535", "17/0xff", Sets.newHashSet(0, 3)),
                        createFilter("10.0.0.0/8", "0.0.0.0/0", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(0)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(1, 2, 3)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:20000", "0:65535", "17/0xff", Sets.newHashSet(1, 3)),
                        createFilter("0.0.0.0/0", "10.0.0.0/8", "0:65535", "0:65535", "17/0xff", Sets.newHashSet(1)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "1000:2000", "0:65535", "17/0xff", Sets.newHashSet(2, 3)),
                        createFilter("0.0.0.0/0", "0.0.0.0/0", "0:20000", "0:65535", "17/0xff", Sets.newHashSet(3)))
                );
    }
 
}
