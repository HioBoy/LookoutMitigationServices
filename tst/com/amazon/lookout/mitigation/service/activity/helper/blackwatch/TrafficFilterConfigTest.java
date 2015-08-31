package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TrafficFilterConfigTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    /**
     * Test parse multiple filters.
     * Test overlap filters are correctly extracted.
     * Test input filter does not have all of the packet attributes.
     * Validate the output filter fill up the missing packet attributes.
     * @throws IOException 
     * @throws JsonMappingException 
     * @throws JsonParseException 
     */
    @Test
    public void testExtractfilters() throws JsonParseException, JsonMappingException, IOException {
        String configStr =    
                  "    {"
                + "       \"traffic_filters\":["
                + "           {"
                + "               \"name\":\"TT_020343423\","
                + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
                + "               \"filter\": {"
                + "                   \"source_ip\":\"1.2.2.1/32\",  "
                + "                   \"dest_ip\":\"54.12.94.110/32\",  "
                + "                   \"source_port\" : \"0:65535\",  "
                + "                   \"dest_port\":\"80:8080\","
                + "                   \"l4_protocol\":\"TCP\""
                + "               }"
                + "           },"
                + "           {"
                + "               \"name\":\"DDOS R53 abc.xyz\","
                + "               \"desc\":\"DDOS attack on route53 for domain name abx.xyz\","
                + "               \"filter\": {"
                + "                   \"dest_ip\":\"54.12.94.0/24\",  "
                + "                   \"source_port\" : \"0:65535\",  "
                + "                   \"dest_port\":\"80\","
                + "                   \"l4_protocol\":\"TCP\""
                + "               }"
                + "           }"
                + "       ]"
                + "   }";
        
        String expectedProcessedConfig =       
                "{\n"
              + "  \"overlap_filters\" : [ {\n"
              + "    \"filter_attributes\" : {\n"
              + "      \"source_ip\" : \"1.2.2.1/32\",\n"
              + "      \"dest_ip\" : \"54.12.94.110/32\",\n"
              + "      \"source_port\" : \"0:65535\",\n"
              + "      \"dest_port\" : \"80:80\",\n"
              + "      \"l4_protocol\" : \"6/0xff\"\n"
              + "    },\n"
              + "    \"related_origin_filter_indices\" : [ 0, 1 ]\n"
              + "  }, {\n"
              + "    \"filter_attributes\" : {\n"
              + "      \"source_ip\" : \"1.2.2.1/32\",\n"
              + "      \"dest_ip\" : \"54.12.94.110/32\",\n"
              + "      \"source_port\" : \"0:65535\",\n"
              + "      \"dest_port\" : \"80:8080\",\n"
              + "      \"l4_protocol\" : \"6/0xff\"\n"
              + "    },\n"
              + "    \"related_origin_filter_indices\" : [ 0 ]\n"
              + "  }, {\n"
              + "    \"filter_attributes\" : {\n"
              + "      \"source_ip\" : \"0.0.0.0/0\",\n"
              + "      \"dest_ip\" : \"54.12.94.0/24\",\n"
              + "      \"source_port\" : \"0:65535\",\n"
              + "      \"dest_port\" : \"80:80\",\n"
              + "      \"l4_protocol\" : \"6/0xff\"\n"
              + "    },\n"
              + "    \"related_origin_filter_indices\" : [ 1 ]\n"
              + "  } ],\n"
              + "  \"traffic_filters\" : [ {\n"
              + "    \"desc\" : \"diagnose traffic to aws customer. TT_020343423\",\n"
              + "    \"filter\" : {\n"
              + "      \"dest_ip\" : \"54.12.94.110/32\",\n"
              + "      \"dest_port\" : \"80:8080\",\n"
              + "      \"l4_protocol\" : \"TCP\",\n"
              + "      \"source_ip\" : \"1.2.2.1/32\",\n"
              + "      \"source_port\" : \"0:65535\"\n"
              + "    },\n"
              + "    \"name\" : \"TT_020343423\"\n"
              + "  }, {\n"
              + "    \"desc\" : \"DDOS attack on route53 for domain name abx.xyz\",\n"
              + "    \"filter\" : {\n"
              + "      \"dest_ip\" : \"54.12.94.0/24\",\n"
              + "      \"dest_port\" : \"80\",\n"
              + "      \"l4_protocol\" : \"TCP\",\n"
              + "      \"source_port\" : \"0:65535\"\n"
              + "    },\n"
              + "    \"name\" : \"DDOS R53 abc.xyz\"\n"
              + "  } ]\n"
              + "}";
        
        assertEquals(mapper.readTree(expectedProcessedConfig), mapper.readTree(TrafficFilterConfiguration.processConfiguration(configStr)));
    }

    /**
     * Test filter field upper lower case mix is allowed
     * @throws IOException 
     * @throws JsonProcessingException 
     */
    @Test
    public void testConfig() throws JsonProcessingException, IOException {
        String configStr =    
                "    {"
              + "       \"traffic_filters\":["
              + "           {"
              + "               \"name\":\"TT_020343423\","
              + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
              + "               \"filter\": {"
              + "                   \"source_iP\":\"1.2.2.1/32\",  "
              + "                   \"source_port\" : \"0:65535\",  "
              + "                   \"dest_port\":\"80\""
              + "               }"
              + "           }"
              + "       ]"
              + "   }"; 
        
        String expectedConfig = 
                  "{\n"
                + "  \"overlap_filters\" : [ {\n"
                + "    \"filter_attributes\" : {\n"
                + "      \"source_ip\" : \"1.2.2.1/32\",\n"
                + "      \"dest_ip\" : \"0.0.0.0/0\",\n"
                + "      \"source_port\" : \"0:65535\",\n"
                + "      \"dest_port\" : \"80:80\",\n"
                + "      \"l4_protocol\" : \"0/0x0\"\n"
                + "    },\n"
                + "    \"related_origin_filter_indices\" : [ 0 ]\n"
                + "  } ],\n"
                + "  \"traffic_filters\" : [ {\n"
                + "    \"desc\" : \"diagnose traffic to aws customer. TT_020343423\",\n"
                + "    \"filter\" : {\n"
                + "      \"dest_port\" : \"80\",\n"
                + "      \"source_iP\" : \"1.2.2.1/32\",\n"
                + "      \"source_port\" : \"0:65535\"\n"
                + "    },\n"
                + "    \"name\" : \"TT_020343423\"\n"
                + "  } ]\n"
                + "}";
        
        assertEquals(mapper.readTree(expectedConfig), mapper.readTree(TrafficFilterConfiguration.processConfiguration(configStr)));
    }
    
    /**
     * Test filter field upper lower case mix is allowed
     * @throws IOException 
     * @throws JsonProcessingException 
     */
    @Test
    public void testParsedConfiguration() throws JsonProcessingException, IOException {
        String configStr = 
                  "{\n"
                + "  \"overlap_filters\" : [ {\n"
                + "    \"filter_attributes\" : {\n"
                + "      \"source_ip\" : \"1.2.2.1/32\",\n"
                + "      \"dest_ip\" : \"0.0.0.0/0\",\n"
                + "      \"source_port\" : \"0:65535\",\n"
                + "      \"dest_port\" : \"80:80\",\n"
                + "      \"l4_protocol\" : \"0/0x0\"\n"
                + "    },\n"
                + "    \"related_origin_filter_indices\" : [ 0 ]\n"
                + "  } ],\n"
                + "  \"traffic_filters\" : [ {\n"
                + "    \"desc\" : \"diagnose traffic to aws customer. TT_020343423\",\n"
                + "    \"filter\" : {\n"
                + "      \"dest_port\" : \"80\",\n"
                + "      \"source_iP\" : \"1.2.2.1/32\",\n"
                + "      \"source_port\" : \"0:65535\"\n"
                + "    },\n"
                + "    \"name\" : \"TT_020343423\"\n"
                + "  } ]\n"
                + "}";
        
        assertEquals(mapper.readTree(configStr), mapper.readTree(TrafficFilterConfiguration.processConfiguration(configStr)));
    } 
    
    /**
     * Test is same config
     * @throws IOException 
     * @throws JsonProcessingException 
     */
    @Test
    public void testIsSameConfig() throws JsonProcessingException, IOException {
        String configStr =    
                "    {"
              + "       \"traffic_filters\":["
              + "           {"
              + "               \"name\":\"TT_020343423\","
              + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
              + "               \"filter\": {"
              + "                   \"source_iP\":\"1.2.2.1/32\",  "
              + "                   \"source_port\" : \"0:65535\",  "
              + "                   \"dest_port\":\"80\""
              + "               }"
              + "           }"
              + "       ]"
              + "   }"; 
        
        String expectedConfig = 
                  "{\n"
                + "  \"overlap_filters\" : [ {\n"
                + "    \"filter_attributes\" : {\n"
                + "      \"source_ip\" : \"1.2.2.1/32\",\n"
                + "      \"dest_ip\" : \"0.0.0.0/0\",\n"
                + "      \"source_port\" : \"0:65535\",\n"
                + "      \"dest_port\" : \"80:80\",\n"
                + "      \"l4_protocol\" : \"0/0x0\"\n"
                + "    },\n"
                + "    \"related_origin_filter_indices\" : [ 0 ]\n"
                + "  } ],\n"
                + "  \"traffic_filters\" : [ {\n"
                + "    \"desc\" : \"diagnose traffic to aws customer. TT_020343423\",\n"
                + "    \"filter\" : {\n"
                + "      \"dest_port\" : \"80\",\n"
                + "      \"source_iP\" : \"1.2.2.1/32\",\n"
                + "      \"source_port\" : \"0:65535\"\n"
                + "    },\n"
                + "    \"name\" : \"TT_020343423\"\n"
                + "  } ]\n"
                + "}";
        assertTrue(TrafficFilterConfiguration.isSameConfig(expectedConfig, TrafficFilterConfiguration.processConfiguration(configStr)));
    } 
    
    /**
     * Test invalid field name, "name1" instead of "name"
     */
    @Test(expected = IllegalArgumentException.class)
    public void invalidConfig() {
        String configStr =    
                "    {"
              + "       \"traffic_filters\":["
              + "           {"
              + "               \"name1\":\"TT_020343423\","
              + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
              + "               \"filter\": {"
              + "                   \"source_ip\":\"1.2.2.1/32\",  "
              + "                   \"dest_ip\":\"54.12.94.110/32\",  "
              + "                   \"source_port\" : \"0:65535\",  "
              + "                   \"dest_port\":\"80:8080\","
              + "                   \"l4_protocol\":\"TCP\""
              + "               }"
              + "           }"
              + "       ]"
              + "   }"; 
        
        TrafficFilterConfiguration.processConfiguration(configStr);
    }
         
    /**
     * Test invalid field name. "Name" instead of "name"
     */
    @Test(expected = IllegalArgumentException.class)
    public void invalidConfig_2() {
        String configStr =    
                "    {"
              + "       \"traffic_filters\":["
              + "           {"
              + "               \"Name\":\"TT_020343423\","
              + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
              + "               \"filter\": {"
              + "                   \"source_ip\":\"1.2.2.1/32\",  "
              + "                   \"dest_ip\":\"54.12.94.110/32\",  "
              + "                   \"source_port\" : \"0:65535\",  "
              + "                   \"dest_port\":\"80:8080\","
              + "                   \"l4_protocol\":\"TCP\""
              + "               }"
              + "           }"
              + "       ]"
              + "   }"; 
        
        TrafficFilterConfiguration.processConfiguration(configStr);
    }
    
    /**
     * Test invalid field name, mis-type field name "souce_ipp" instead of "source_ip"
     */
    @Test(expected = IllegalArgumentException.class)
    public void invalidConfig_3() {
        String configStr =    
                "    {"
              + "       \"traffic_filters\":["
              + "           {"
              + "               \"name\":\"TT_020343423\","
              + "               \"desc\":\"diagnose traffic to aws customer. TT_020343423\","
              + "               \"filter\": {"
              + "                   \"source_ipp\":\"1.2.2.1/32\",  "
              + "                   \"dest_ip\":\"54.12.94.110/32\",  "
              + "                   \"source_port\" : \"0:65535\",  "
              + "                   \"dest_port\":\"80:8080\","
              + "                   \"l4_protocol\":\"TCP\""
              + "               }"
              + "           }"
              + "       ]"
              + "   }"; 
        
        TrafficFilterConfiguration.processConfiguration(configStr);
    }
}