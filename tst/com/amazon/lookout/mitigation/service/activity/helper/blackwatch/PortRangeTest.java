package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PortRangeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_1() {
        PortRange.fromString("fdas1213dwe");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_2() {
        PortRange.fromString("1000:65536");
    }
     
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_3() {
        PortRange.fromString("-1:65535");
    }
   
    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_4() {
        PortRange.fromString("1:-65535");
    }
   
    @Test
    public void testOverlap_1() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("0:29999");
        
        assertNull(p1.findOverlap(p2));
    }

    /**
     * one port overlap
     */
    @Test
    public void testOverlap_2() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("0:30000");

        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(30000, p3.getUpperEnd());
        
        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(30000, p3.getUpperEnd());
    }
  
    /**
     * a range of ports overlap
     */
    @Test
    public void testOverlap_4() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("0:40000");
        
        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());
        
        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());
    }
     
    /**
     * two overlap, one include another, and with same lower end
     */
    @Test
    public void testOverlap_5() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("30000:40000");
        
        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());

        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());
    }
    
    /**
     * two overlap, one include another, and with same upper end
     */ 
    @Test
    public void testOverlap_6() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("50000:65535");
        
        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(50000, p3.getLowerEnd());
        assertEquals(65535, p3.getUpperEnd());
         
        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(50000, p3.getLowerEnd());
        assertEquals(65535, p3.getUpperEnd());
    }
    
    /**
     * two overlap, one inside another 
     */
    @Test
    public void testOverlap_7() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("35000:40000");
        
        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(35000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());

        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(35000, p3.getLowerEnd());
        assertEquals(40000, p3.getUpperEnd());
    }
    
     
    /**
     * two overlap, exactly same
     */
    @Test
    public void testOverlap_8() {
        PortRange p1 = PortRange.fromString("30000:65535");
        PortRange p2 = PortRange.fromString("30000:65535");
        
        PortRange p3 = (PortRange)p1.findOverlap(p2);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(65535, p3.getUpperEnd());

        p3 = (PortRange)p2.findOverlap(p1);
        assertEquals(30000, p3.getLowerEnd());
        assertEquals(65535, p3.getUpperEnd());
    }

    @Test
    public void testToString() {
        PortRange p1 = PortRange.fromString("    30000 : 65535   ");
        assertEquals("30000:65535", p1.toString());
    }

    @Test
    public void testSerializer() throws JsonProcessingException {
        PortRange p1 = PortRange.fromString("    30000 : 65535   ");
        assertEquals("\"30000:65535\"", new ObjectMapper().writeValueAsString(p1));
    }
}
