package com.trademarketx.springwebflux.commons.conversion;

import org.springframework.boot.jackson.JacksonComponent;
import io.r2dbc.postgresql.codec.Json;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

@JacksonComponent
public class R2dbcJsonJackson {
    public static class Serializer extends ValueSerializer<Json> {
        @Override
        public void serialize(Json value, JsonGenerator generator, SerializationContext context) {
            generator.writeRawValue(value.asString());
        }
    }
    public static class Deserializer extends ValueDeserializer<Json> {
        @Override
        public Json deserialize(JsonParser parser, DeserializationContext context) {
            JsonNode node = parser.readValueAsTree();
            return Json.of(node.toString());
        }
    }
}
