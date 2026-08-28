package com.trademarketx.springwebflux.commons.conversion;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Row;

@Component
public class MapConversion {

    private final ObjectMapper objectMapper;

    public MapConversion(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> rowToMap(Row row) {

        if (row == null) return Map.of();

        Map<String, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < row.getMetadata().getColumnMetadatas().size(); i++) {

            String columnName = row.getMetadata().getColumnMetadatas().get(i).getName();
            Object value = row.get(i);

            if (value instanceof Json json) {
                try {
                    result.put(columnName, objectMapper.readValue(json.asString(), new TypeReference<>() {}));
                } catch (Exception e) {
                    result.put(columnName, json.asString());
                }
            } else {
                result.put(columnName, value);
            }
        }

        return result;
    }
}