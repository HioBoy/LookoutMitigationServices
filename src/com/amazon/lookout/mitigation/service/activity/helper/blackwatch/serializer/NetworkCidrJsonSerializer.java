package com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer;

import java.io.IOException;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.NetworkCidr;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * handle NetworkCidr object json serialize
 * @author xingbow
 *
 */
public class NetworkCidrJsonSerializer extends JsonSerializer<NetworkCidr> {
    @Override
    public void serialize(NetworkCidr value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        jgen.writeString(value.toString());
    }
}
