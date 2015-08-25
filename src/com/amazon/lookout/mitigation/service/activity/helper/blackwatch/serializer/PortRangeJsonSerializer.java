package com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer;

import java.io.IOException;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.PortRange;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * handle port range object json serialize
 * @author xingbow
 *
 */
public class PortRangeJsonSerializer extends JsonSerializer<PortRange> {

    @Override
    public void serialize(PortRange value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeString(value.toString());
    }

}

