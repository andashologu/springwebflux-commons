package com.trademarketx.springwebflux.commons.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.r2dbc.postgresql.codec.Json;

import java.io.IOException;

@Configuration
public class JacksonConfig {
    @Bean
    public ObjectMapper objectMapper() {
        SimpleModule jsonModule = new SimpleModule();
        jsonModule.addSerializer(Json.class, new JsonSerializer<Json>() {
            @Override
            public void serialize(Json value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeRawValue(value.asString());
            }
        });
        jsonModule.addDeserializer(Json.class, new JsonDeserializer<Json>() {
            @Override
            public Json deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                return Json.of(parser.readValueAsTree().toString());
            }
        });
        return new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE)
            .registerModule(new JavaTimeModule())
            .registerModule(jsonModule)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}