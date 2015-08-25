package com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer;

import java.io.IOException;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.L4Protocol;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * handle L4 protocol object json serialize
 * @author xingbow
 *
 */
public class L4ProtocolJsonSerializer extends JsonSerializer<L4Protocol> {
    @Override
    public void serialize(L4Protocol value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeString(value.toString());
    }
}
