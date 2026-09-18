package com.trademarketx.springwebflux.commons.conversion;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trademarketx.springwebflux.commons.util.Util;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Row;
import org.springframework.data.annotation.Id;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import java.lang.reflect.Field;
import java.util.Map;
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
                Object value;
                if (field.getType() == Map.class) {
                    Json json = row.get(column, Json.class);
                    value = json != null
                        ? objectMapper.readValue(json.asString(), Map.class)
                        : null;
                }
                else if (Util.isJsonType(field.getType())) {
                    Json json = row.get(column, Json.class);
                    value = json != null
                        ? objectMapper.readValue(json.asString(), field.getType())
                        : null;
                }
                else if (field.getType().isEnum()) {
                    Object raw = row.get(column);
                    value = raw != null ? EnumConversion.toEnum(field, raw.toString()) : null;
                }
                else if (field.getType().isArray()) {
                    value = row.get(column, field.getType());
                }
                else {
                    value = row.get(column, field.getType());
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
                if (field.getType() == Map.class || Util.isJsonType(field.getType())) {
                    if (value == null) {
                        spec = spec.bindNull(column, Json.class);
                    } else {
                        spec = spec.bind(column, Json.of(objectMapper.writeValueAsString(value)));
                    }
                }
                else if (field.getType().isEnum()) {
                    if (value == null) {
                        spec = spec.bindNull(column, String.class);
                    } else {
                        spec = spec.bind(column, ((Enum<?>) value).name());
                    }
                }
                else if (field.getType().isArray()) {
                    if (value == null) {
                        spec = spec.bindNull(column, field.getType());
                    } else {
                        spec = spec.bind(column, value);
                    }
                }
                else {
                    if (value == null) {
                        spec = spec.bindNull(column, field.getType());
                    } else {
                        spec = spec.bind(column, value);
                    }
                }
            }
            return spec;
        } catch (Exception e) {
            throw new RuntimeException("Failed to bind entity values", e);
        }
    }
}