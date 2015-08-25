package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class L4ProtocolTest {

    @Test
    public void testProtocolName() {
        L4Protocol tcp = L4Protocol.fromString("TCP");
        assertEquals(0xff, tcp.getMask());
        assertEquals(6, tcp.getValue());
         
        L4Protocol udp = L4Protocol.fromString("UDP");
        assertEquals(0xff, udp.getMask());
        assertEquals(17, udp.getValue());
        
        L4Protocol icmp = L4Protocol.fromString("ICMP");
        assertEquals(0xff, icmp.getMask());
        assertEquals(1, icmp.getValue());
        
        L4Protocol proto = L4Protocol.fromString("16 \t  / 0xf0");
        assertEquals(0xf0, proto.getMask());
        assertEquals(16, proto.getValue());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput() {
        L4Protocol.fromString("312 rweqj;");
    }
    
    @Test
    public void testOverlap_1() {
        L4Protocol tcp = L4Protocol.fromString("TCP");
        L4Protocol udp = L4Protocol.fromString("UDP");
        assertNull(tcp.findOverlap(udp));
    }
    
    @Test
    public void testOverlap_2() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        L4Protocol p2 = L4Protocol.fromString("5/0x07");
        L4Protocol p3 = (L4Protocol)p1.findOverlap(p2);
        assertEquals(0xd7, p3.getMask());
        assertEquals(85, p3.getValue());
                
        p3 = (L4Protocol)p2.findOverlap(p1);
        assertEquals(0xd7, p3.getMask());
        assertEquals(85, p3.getValue());
    }
    
    @Test
    public void testOverlap_22() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        L4Protocol p2 = L4Protocol.fromString("0/0x0");
        assertEquals(p1, p1.findOverlap(p2));
        assertEquals(p1, p2.findOverlap(p1));
        assertEquals(p1, p1.findOverlap(p1));
        assertEquals(p2, p2.findOverlap(p2));
    }
    
    @Test
    public void testOverlap_3() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        L4Protocol p2 = L4Protocol.fromString("5/0x87");
        L4Protocol p3 = (L4Protocol)p1.findOverlap(p2);
        assertEquals(0xd7, p3.getMask());
        assertEquals(85, p3.getValue());

        p3 = (L4Protocol)p2.findOverlap(p1);
        assertEquals(0xd7, p3.getMask());
        assertEquals(85, p3.getValue());
    } 
     
    @Test
    public void testOverlap_4() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        L4Protocol p2 = L4Protocol.fromString("133/0x87");
        assertNull(p1.findOverlap(p2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOverlap_5() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        p1.findOverlap(null);
    }
    
    @Test
    public void testOverlap_6() {
        L4Protocol p1 = L4Protocol.fromString("80/0xd0");
        L4Protocol p2 = L4Protocol.fromString("80/0xd0");
        L4Protocol p3 = (L4Protocol)p1.findOverlap(p2);
        assertEquals(0xd0, p3.getMask());
        assertEquals(80, p3.getValue());
        
        p3 = (L4Protocol)p2.findOverlap(p1);
        assertEquals(0xd0, p3.getMask());
        assertEquals(80, p3.getValue());
    }
    
    @Test
    public void testToString() {
        L4Protocol p1 = L4Protocol.fromString("80 /   0xd0");
        assertEquals("80/0xd0", p1.toString());
    }
    
    @Test
    public void testJackson() throws JsonProcessingException {
        L4Protocol p1 = L4Protocol.fromString("80 /   0xd0");
        ObjectMapper mapper = new ObjectMapper();
        assertEquals("\"80/0xd0\"", mapper.writeValueAsString(p1));
    }
}
