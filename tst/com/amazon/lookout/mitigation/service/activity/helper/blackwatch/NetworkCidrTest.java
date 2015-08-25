package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NetworkCidrTest {

    @Test
    public void testConstructor() {
        NetworkCidr cidr = NetworkCidr.fromString("0.0.0.0 /\t0");
        assertEquals(0, cidr.getDepth());
        assertEquals(0, cidr.getMask());
        assertEquals(0, cidr.getPrefix());

        cidr = NetworkCidr.fromString("\t1.1.1.1/32");
        assertEquals(32, cidr.getDepth());
        assertEquals(0xffffffff, cidr.getMask());
        assertEquals(0x01010101, cidr.getPrefix());

        cidr = NetworkCidr.fromString(" 12.128.0.0/16 \t");
        assertEquals(16, cidr.getDepth());
        assertEquals(0xffff0000, cidr.getMask());
        assertEquals(0x0c800000, cidr.getPrefix());
    }
   
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput_1() {
        NetworkCidr.fromString("0.0.0.0/33");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput_2() {
        NetworkCidr.fromString("1.1.1.1/24");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput_3() {
        NetworkCidr.fromString("256.0.0.0/24");
    }
  
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput_4() {
        NetworkCidr.fromString("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput_5() {
        NetworkCidr.fromString("fdsafas32e2ee32");
    }
    
    @Test
    public void testOverlap_1() {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0/8");
        NetworkCidr cidr2 = NetworkCidr.fromString("10.0.12.0/24");
        assertEquals(cidr2, cidr1.findOverlap(cidr2));
        assertEquals(cidr2, cidr2.findOverlap(cidr1));
        assertEquals(cidr2, cidr2.findOverlap(cidr2));
        assertEquals(cidr1, cidr1.findOverlap(cidr1));
    }
        
    @Test
    public void testOverlap_2() {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0/24");
        NetworkCidr cidr2 = NetworkCidr.fromString("10.0.12.0/24");
        assertNull(cidr1.findOverlap(cidr2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOverlap_3() {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0/24");
        cidr1.findOverlap(null);
    }
    
    @Test
    public void testOverlap_4() {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0/8");
        NetworkCidr cidr2 = NetworkCidr.fromString("10.0.0.0/8");
        assertEquals(cidr2, cidr1.findOverlap(cidr2));
        assertEquals(cidr2, cidr2.findOverlap(cidr1));
    }
    
    @Test
    public void testToString() {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0 / 8");
        assertEquals("10.0.0.0/8", cidr1.toString());
    }
    
    @Test
    public void testJsonSerializer() throws JsonProcessingException {
        NetworkCidr cidr1 = NetworkCidr.fromString("10.0.0.0 / 8");
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("\"10.0.0.0/8\"", mapper.writeValueAsString(cidr1));
    }
}
