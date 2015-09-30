package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.commons.lang.Validate;

import lombok.AllArgsConstructor;
import lombok.Getter;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.TrafficFilter.AttributeName;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Process BlackWatch Traffic Filter configuration from user input.
 * Process include:
 * parse and validate JSON configuration
 * extract overlap filters from configuration
 * generate new JSON configuration, which include user input configuration and the extracted overlap filters.
 * 
 * @author xingbow
 */
public class TrafficFilterConfiguration {
    
    private static final String ORIGIN_TRAFFIC_FILTER_FIELD_NAME = "traffic_filters";
    
    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        // ignore missing fields
        mapper.setSerializationInclusion(Include.NON_NULL);
        // convert json name style to java name style
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
        // use toString method to enum, which will lower case all enum name.
        mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    }
    
    /**
     * origin traffic filter, used for parsing and validating input traffic filter in JSON configuration
     */
    @Getter
    @SuppressWarnings("UWF_UNWRITTEN_FIELD")
    private static class OriginTrafficFilter {
        private String name;
        private String desc;
        private Map<String, String>filter;
    }
    
    /**
     * root configuration, used for parsing and validating input traffic filter in JSON configuration
     */
    @AllArgsConstructor
    @Getter
    private static class RootConfiguration {
        private List<OriginTrafficFilter> trafficFilters;
        private List<TrafficFilter> overlapFilters;
    }

    /**
     * we loop through each field, and covert it to PacketAttribute Object.
     * If any unknown or mis-typed field appear, we will throw IllegalArgumentException, to avoid user mis-typing.
     * Missing field is fine, they will be filled with default value later.
     * This does require each field's name is in lower case and under score style in config file,
     * and its AttributeName enum is defined in corresponding upper case and under score style.
     */
    private static Map<AttributeName, PacketAttribute> createAttribute(Map<String, String> filter) {
        Map<AttributeName, PacketAttribute> packetAttributes = new EnumMap<>(AttributeName.class);
        for (Map.Entry<String, String> fieldNameValuePair : filter.entrySet()) {
            String fieldName = fieldNameValuePair.getKey();
            String value = fieldNameValuePair.getValue();
            try {
                AttributeName attributeName = AttributeName.valueOf(fieldName.toUpperCase());
                packetAttributes.put(attributeName, attributeName.getType().createPacketAttribute(value));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(String.format("field %s is not supported by BlackWatch Traffic filter. "
                        + "Supported fields : %s", fieldName, Arrays.asList(AttributeName.values())), ex);
            }
        }
        return packetAttributes;
    }
    
    /**
     * Process configuration.
     * Take origin configuration string.
     * Parse, validate configuration.
     * Extract overlap filters from configuration.
     * return processed new configuration.
     * @param originConfig : origin configuration JSON string
     * @return processed configuration JSON string
     */
    public static String processConfiguration (String originConfig) {
        try {
            // extract only origin filters from origin configuration
            Map<String, Object> inputConfig = mapper.readValue(originConfig, new TypeReference<HashMap<String, Object>>(){});
            Object trafficFilters = inputConfig.get(ORIGIN_TRAFFIC_FILTER_FIELD_NAME);
            String trafficFiltersStr = mapper.writeValueAsString(trafficFilters);
            List<OriginTrafficFilter> origins = mapper.readValue(trafficFiltersStr, new TypeReference<ArrayList<OriginTrafficFilter>>(){});
            
            List<Map<AttributeName, PacketAttribute>> originAttributesList = new ArrayList<>();
            for (OriginTrafficFilter originFilter : origins) {
                Map<AttributeName, PacketAttribute> packetAttributes = createAttribute(originFilter.getFilter());
                if (!packetAttributes.isEmpty()) {
                    originAttributesList.add(packetAttributes);
                } else {
                    throw new IllegalArgumentException("Found a filter with empty definition. Configuration : " + originConfig);
                }
            }
            
            RootConfiguration processedConfiguration = new RootConfiguration(origins, OverlapTrafficFiltersExtractor.extractOverlapFilters(originAttributesList));
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(processedConfiguration);
        } catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Failed to parse traffic filter configuration, config : %s", originConfig), ex);
        }
    }

    public static boolean isSameConfig(String originConfig, String processedConfig) {
        Validate.notEmpty(originConfig);
        Validate.notEmpty(processedConfig);
        try {
            return mapper.readTree(originConfig).equals(mapper.readTree(processedConfig));
        } catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Falied to check equality between origin config and processed config "), ex);
        }
    }
}
