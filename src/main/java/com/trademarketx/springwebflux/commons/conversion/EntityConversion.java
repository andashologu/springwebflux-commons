package com.trademarketx.springwebflux.commons.conversion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trademarketx.springwebflux.commons.util.Util;

import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Row;
import org.springframework.data.annotation.Id;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

@Component
public class EntityConversion {

    private final ObjectMapper objectMapper;

    public EntityConversion(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <T> T rowToEntity(Row row, T entity) {

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (Util.isTransient(field)) continue;

            field.setAccessible(true);
            String column = field.getName();

            try {
/*
                Object value;

                if (field.getType().isEnum()) {
                    value = row.get(column); // <-- CRITICAL FIX
                } else {
                    value = row.get(column, field.getType());
                }

                if (value == null && Util.isJsonType(field.getType())) {
                    Object raw = row.get(column);
                    if (raw != null) {
                        value = objectMapper.convertValue(raw, field.getType());
                    }
                }
*/
                Object value;

                if (Util.isJsonType(field.getType())) {

                    Object raw = row.get(column);

                    if (raw instanceof Json json) {
                        String jsonString = json.asString();
                        value = objectMapper.readValue(jsonString, field.getType());
                    } else if (raw != null) {
                        value = objectMapper.convertValue(raw, field.getType());
                    } else {
                        value = null;
                    }

                } else if (field.getType().isEnum()) {

                    Object raw = row.get(column);
                    value = raw != null ? EnumConversion.toEnum(field, raw.toString()) : null;

                } else {

                    value = row.get(column, field.getType());
                }

                if (field.getType().isEnum() && value != null) {
                    value = EnumConversion.toEnum(field, value.toString());
                }

                field.set(entity, value);

            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to map column '" + column + "' to field '" + field.getName() + "'", e
                );
            }
        }
        return entity;
    }

    public <T> DatabaseClient.GenericExecuteSpec entityToRow(DatabaseClient.GenericExecuteSpec spec, T entity) {
        try {
            for (Field field : entity.getClass().getDeclaredFields()) {

                if (Util.isTransient(field) || field.isAnnotationPresent(Id.class)) continue;

                field.setAccessible(true);
                Object value = field.get(entity);
                String column = field.getName();

                if (value == null) {

                    if (Util.isJsonType(field.getType())) {
                        spec = spec.bindNull(column, Json.class);
                    } else {
                        spec = spec.bindNull(column, Object.class);
                    }

                } else if (field.getType().isEnum()) {

                    spec = spec.bind(column, ((Enum<?>) value).name());

                } else if (Util.isJsonType(field.getType())) {

                    spec = spec.bind(column,
                        Json.of(objectMapper.writeValueAsString(value)));

                } else {

                    spec = spec.bind(column, value);
                }
            }
            return spec;

        } catch (Exception e) {
            throw new RuntimeException("Failed to bind entity values", e);
        }
    }
}