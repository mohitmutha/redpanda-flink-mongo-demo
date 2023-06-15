package com.dev.poc.flink.serde;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.dev.poc.flink.dto.NumEvent;
import java.io.IOException;

public class NumEventDeserializationSchema implements DeserializationSchema<NumEvent> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public NumEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, NumEvent.class);
    }

    @Override
    public boolean isEndOfStream(NumEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<NumEvent> getProducedType() {
        return TypeInformation.of(NumEvent.class);
    }
}