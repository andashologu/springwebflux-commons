package com.trademarketx.springwebflux.commons;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.annotation.Id;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trademarketx.springwebflux.commons.conversion.EntityConversion;
import com.trademarketx.springwebflux.commons.conversion.MapConversion;
import com.trademarketx.springwebflux.commons.util.Util;

import reactor.core.publisher.Flux;

import reactor.core.publisher.Mono;

@Component
public class CustomRepository<T, ID> {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;
    private final EntityConversion entityConversion;
    private final MapConversion mapConversion;

    public CustomRepository(DatabaseClient databaseClient, ObjectMapper objectMapper, EntityConversion entityConversion, MapConversion mapConversion) {
        this.databaseClient = databaseClient;
        this.objectMapper = objectMapper;
        this.entityConversion = entityConversion;
        this.mapConversion = mapConversion;
    }
    private static String q(String identifier) {
        return "\"" + identifier + "\"";
    }

    /********************************************
     *                                          *
     *                 create                   *
     *                                          *
     ********************************************/

    public Mono<T> save(T entity) {
        // IO.print("\nCustomRepository.java > save() > entity: " + entity.toString());

        Field idField = findIdField(entity.getClass());

        try {
            Object idValue = idField.get(entity);

            if (idValue == null) {
                // INSERT
                String sql = generateInsertSql(entity.getClass());
                // IO.print("\nCustomRepository.java > save() > INSERT SQL: " + sql);

                DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql);
                spec = entityConversion.entityToRow(spec, entity);

                return spec
                    .map((row, _) -> entityConversion.rowToEntity(row, entity))
                    .one();

            } else {
                // UPDATE
                String sql = generateUpdateSql(entity.getClass(), idField);
                // IO.print("\nCustomRepository.java > save() > UPDATE SQL: " + sql);

                DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql);
                spec = entityConversion.entityToRow(spec, entity);
                spec = spec.bind(idField.getName(), idValue);

                return spec
                    .map((row, _) -> entityConversion.rowToEntity(row, entity))
                    .one();
            }

        } catch (IllegalAccessException e) {
            return Mono.error(e);
        }
    }

    public String generateInsertSql(Class<?> entity) {

        String tableName = Util.getTableName(entity);

        List<String> columns = new ArrayList<>();
        List<String> params = new ArrayList<>();
        List<String> returning = new ArrayList<>();

        for (Field field : entity.getDeclaredFields()) {

            if (Util.isTransient(field)) continue;

            String columnName = field.getName();

            if (field.isAnnotationPresent(Id.class)) {
                returning.add(columnName);
                continue;
            }

            columns.add(columnName);
            params.add(":" + columnName);
            returning.add(columnName);
        }

        return "INSERT INTO " + q(tableName) +
            " (" + columns.stream().map(CustomRepository::q).collect(Collectors.joining(", ")) + ")" +
            " VALUES (" + String.join(", ", params) + ")" +
            " RETURNING " + returning.stream().map(CustomRepository::q).collect(Collectors.joining(", "));
    }

    private String generateUpdateSql(Class<?> entity, Field idField) {

        String tableName = Util.getTableName(entity);

        List<String> sets = new ArrayList<>();
        List<String> returning = new ArrayList<>();

        for (Field field : entity.getDeclaredFields()) {

            if (Util.isTransient(field) || field.isAnnotationPresent(Id.class)) continue;

            String column = field.getName();
            sets.add(q(column) + " = :" + column);
            returning.add(q(column));
        }

        returning.add(q(idField.getName()));

        return "UPDATE " + q(tableName) +
            " SET " + String.join(", ", sets) +
            " WHERE " + q(idField.getName()) + " = :" + idField.getName() +
            " RETURNING " + String.join(", ", returning);
    }

    private Field findIdField(Class<?> entityClass) {
        for (Field f : entityClass.getDeclaredFields()) {
            if (f.isAnnotationPresent(Id.class)) {
                f.setAccessible(true);
                return f;
            }
        }
        throw new IllegalStateException("No @Id field found in " + entityClass.getSimpleName());
    }
  
    /********************************************
     *                                          *
     *                 UPDATE                   *
     *                                          *
     ********************************************/

    public Mono<Map<String, Object>> patch(ID primaryId, String secondaryIdColumnName, ID secondaryId, Class<?> entityClass, Map<String, Object> fieldsToUpdate) {
        if (fieldsToUpdate == null || fieldsToUpdate.isEmpty()) {
            return Mono.error(new IllegalArgumentException("No fields to update"));
        }

        String tableName = Util.getTableName(entityClass);

        Map<String, Field> keyToField = new HashMap<>();
        for (Field field : entityClass.getDeclaredFields()) {
            if (Util.isTransient(field)) continue;
            field.setAccessible(true);
            keyToField.put(field.getName(), field);
        }

        Map<String, Object> bindings = new LinkedHashMap<>();
        List<String> setClauses = new ArrayList<>();

        for (Map.Entry<String, Object> entry : fieldsToUpdate.entrySet()) {

            String inputKey = entry.getKey();
            Object value    = entry.getValue();

            Field field = keyToField.get(inputKey);
            if (field == null) {
                return Mono.error(new IllegalArgumentException("Unknown field: " + inputKey));
            }

            String columnName = field.getName();
            String paramName  = field.getName();

            if (value instanceof Map<?, ?> jsonMap) {
                String clause =
                    buildNestedJsonbSet(columnName, jsonMap, bindings, paramName);
                setClauses.add(q(columnName) + " = " + clause);
            } else {
                //setClauses.add(columnName + " = :" + paramName);
                setClauses.add(q(columnName) + " = :" + paramName);
                bindings.put(paramName, value);
            }
        }

        String setSql = String.join(", ", setClauses);

        String returningClause = buildJsonbReturnClause(tableName, fieldsToUpdate, keyToField, null);

        //String whereClause = " WHERE id = :id";
        String whereClause = " WHERE " + q("id") + " = :id";
        bindings.put("id", primaryId);

        if (secondaryIdColumnName != null && secondaryId != null) {
            //whereClause += " AND " + secondaryIdColumnName + " = :secondaryId";
            whereClause += " AND " + q(secondaryIdColumnName) + " = :secondaryId";
            bindings.put("secondaryId", secondaryId);
        }

        String sql =
            "UPDATE " + q(tableName) +
            " SET " + setSql +
            whereClause +
            " RETURNING " + returningClause + " AS updates";
        
        // IO.print("\nCustomRepository.class: patch() sql:" + sql); // DONT DELETE

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql);
        for (Map.Entry<String, Object> bind : bindings.entrySet()) {
            spec = bind.getValue() == null
                ? spec.bindNull(bind.getKey(), Object.class)
                : spec.bind(bind.getKey(), bind.getValue());
        }

        return spec
            .map((row, _) -> mapConversion.rowToMap(row))
            .first(); // ← empty Mono if no row updated.. so that switchIfEmpty can be used
    }

    private String buildNestedJsonbSet(
            String column,
            Map<?, ?> nestedKey,
            Map<String, Object> bindings,
            String prefix
    ) {
        String expr = "coalesce(" + q(column) + ", '{}'::jsonb)";
        for (Map.Entry<?, ?> entry : nestedKey.entrySet()) {
            String key = entry.getKey().toString();
            Object valueObject = entry.getValue();
            String paramName = prefix + "_" + key;

            if (valueObject instanceof Map<?, ?> valueMap) {
                expr = buildNestedJsonbSet(expr, valueMap, bindings, paramName);
            } else {

                String regular_key = "";
                if(valueObject != null) {
                    regular_key = valueObject.toString();
                } 
                else {
                    IO.print("\nCustomRepository.java > buildNestedJsonbSet() > valueObject: " + valueObject);
                }
                String cast = "::text";
                String value = regular_key;

                if (regular_key.contains("::")) {
                    String[] parts = regular_key.split("::", 2);
                    value = parts[0];
                    cast = "::" + parts[1];
                }

                // Bind the literal (without cast)
                bindings.put(paramName, value);

                // Build jsonb_set with explicit cast
                expr = "jsonb_set(" + expr + ", '{" + key + "}', to_jsonb(:" + paramName + cast + "), true)";
            }
        }
        return expr;
    }

    private String buildJsonbReturnClause(
    String tableName,
    Map<String, Object> resultsMap,
    Map<String, Field> keyToField,
    String parentExpr
) {
    return "jsonb_build_object(" +
        resultsMap.entrySet().stream()
            .map(entry -> {

                String inputKey = entry.getKey();
                Object value = entry.getValue();

                Field field = keyToField.get(inputKey);

                // 🔒 validate ONLY top-level entity fields
                if (parentExpr == null && field == null) {
                    throw new IllegalArgumentException(
                        "Unknown field in RETURNING clause: " + inputKey
                    );
                }

                String jsonKey = (field != null)
                    ? field.getName()
                    : inputKey;

                String expr = (parentExpr == null)
                    ? q(tableName) + "." + q(jsonKey)
                    : parentExpr + " -> '" + jsonKey + "'";

                if (value instanceof Map<?, ?> nested) {
                    Map<String, Object> nestedMap =
                        objectMapper.convertValue(nested, new TypeReference<>() {});

                    return "'" + jsonKey + "', " +
                        buildJsonbReturnClause(
                            tableName,
                            nestedMap,
                            keyToField,
                            expr
                        );
                }

                return "'" + jsonKey + "', " + expr;
            })
            .collect(Collectors.joining(", ")) +
        ")";
}

    /********************************************
     *                                          *
     *                 GET                      *
     *                                          *
     ********************************************/

    public Flux<Map<String, Object>> get(Class<?> entityClass, String query, Boolean isInternalRequest) {     
        //ObjectMapper objectMapper = new ObjectMapper();
        QueryDTO queryDTO = null;

        try {
            queryDTO = objectMapper.readValue(query, QueryDTO.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Invalid query JSON", e);
        }
        
        return getAny(
            entityClass,
            queryDTO.getAssociations(),
            queryDTO.getResults(),
            queryDTO.getSearch(),
            queryDTO.getSearchFields(),
            queryDTO.getFilters(),
            queryDTO.getFiltersOperator(),
            queryDTO.getSorting(),
            queryDTO.getSize(),
            queryDTO.getCursor(),
            isInternalRequest
        );
    }
    /*
        not designed to express mutually exclusive sources - produced row multiplication. 
    */
    public Flux<Map<String, Object>> getAny(
            Class<?> entityClass,
            Map<String, Object> associationsMap,
            Map<String, Object> resultsMap,
            String search,
            Map<String, List<String>> searchFieldsMap,
            Map<String, Object> filtersMap,
            String filtersOperator,
            Map<String, List<Map<String, String>>> sortMap, 
            Integer size,
            Long cursor,
            Boolean isInternalRequest
    ) {
        try {
            Map<String, Object> binds = new HashMap<>();
            Set<String> allowedTables = new HashSet<>();

            String primaryTable = Util.getTableName(entityClass);

            if(!isInternalRequest){
                //primaryTable = primaryTable + "_safe_view";
                primaryTable = isInternalRequest
                    ? primaryTable
                    : primaryTable + "SafeView";
            }

           allowedTables.add(primaryTable);

            String selectClause = "*"; // dummy column
            String joinClause = "";
            StringBuilder whereClause = new StringBuilder();
            String orderByClause = "";

            // --- Build SELECT clause ---

            /*
             * 
             * results
             * 
             */
            if (resultsMap != null && !resultsMap.isEmpty()) {
                List<String> expressions = new ArrayList<>();

                for (Map.Entry<String, Object> result : resultsMap.entrySet()) {
                    String table = result.getKey();

                    List<Object> columnsObject = objectMapper.readValue(
                            objectMapper.writeValueAsString(result.getValue()),
                            new TypeReference<List<Object>>() {}
                    );

                    if (columnsObject == null) {
                        throw new IllegalStateException("\nCustumRepository.java > getAny() > columnsObject is null for table: " + table);
                    }

                    String jsonbObject;

                    if (columnsObject.isEmpty()) {
                        // Empty list means “select all columns”
                        jsonbObject = "to_jsonb(" + q(table) + ")";
                    } else {
                        // Build only specific columns
                        jsonbObject = buildResults(table, columnsObject);
                    }

                    expressions.add(jsonbObject + " AS " + table);
                }

                selectClause = String.join(", ", expressions);
            }

            // --- Build JOIN clause ---
            /*
             * 
             * associations
             * 
             */
            if(associationsMap != null && !associationsMap.isEmpty()){
                for (String parent : associationsMap.keySet()) {
                    allowedTables.add(parent);
                    Object tableObj = associationsMap.get(parent);
                    if (tableObj instanceof Map<?, ?> mapObj) {
                        Object relations = mapObj.get("relations");
                        if (relations instanceof List<?> relList) {
                            relList.forEach(r -> allowedTables.add(r.toString()));
                        }
                    }
                }
                joinClause = buildJoins(associationsMap, primaryTable, isInternalRequest);
            }
            
            // --- WHERE clause ---
            /*
             * 
             * filters
             * 
             */
            if (filtersMap != null && !filtersMap.isEmpty()){
                /*for (String table : filtersMap.keySet()) {
                    if (!allowedTables.contains(table)) 
                        throw new IllegalArgumentException(
                            "F\nCustomRepository.java > getAny() > ilter references table '" + table + "' which is not in associations or primary table");
                }*/
            /*    
                whereClause.append(buildFilters(filtersMap, filtersOperator, binds));
            */
                whereClause.append(" AND ");
                whereClause.append(buildFilters(filtersMap, binds));

            }

            /*
             * 
             * seaching
             * 
             */
            if (search != null && !search.isBlank() && searchFieldsMap != null && !searchFieldsMap.isEmpty()){
                for (String table : searchFieldsMap.keySet())
                    if (!allowedTables.contains(table)) throw new IllegalArgumentException(
                            "Search references table '" + table + "' which is not in associations or primary table");
                whereClause.append(buildSearch(search, searchFieldsMap, binds));
            }

            // --- ORDER BY clause ---
            /*
             * 
             * sorting
             * 
             */
            if (sortMap != null && !sortMap.isEmpty()) {
                for (String table : sortMap.keySet()) {
                    if (!allowedTables.contains(table)) throw new IllegalArgumentException(
                            "Sorting references table '" + table + "' which is not in associations or primary table");
                }
                orderByClause = buildSort(sortMap/*, binds*/, search, searchFieldsMap);
            }

            // --- Pagination ---
            /*
             * 
             * pagination
             * 
             */
            String limitClause = (size != null) ? " LIMIT :limit" : "";
            //String cursorFilter = (cursor != null) ? " AND " + primaryTable + ".id > :cursor" : "";
            String cursorFilter = (cursor != null)
                ? " AND " + q(primaryTable) + "." + q("id") + " > :cursor"
                : "";


            // --- Final SQL ---
            /*
             * 
             * FINAL
             * 
             */
            String sql = "SELECT " + selectClause +
                        " FROM " + q(primaryTable) +
                        " " + joinClause +
                        " WHERE 1=1 " + whereClause + cursorFilter +
                        orderByClause + limitClause;

            // --- Execute query ---
            DatabaseClient.GenericExecuteSpec exec = databaseClient.sql(sql);
            if (size != null) exec = exec.bind("limit", size);
            if (cursor != null) exec = exec.bind("cursor", cursor);
            if (search != null) exec = exec.bind("search", search);
            for (var entry : binds.entrySet()) {
                String key = entry.getKey();
                Object value =entry.getValue();
                exec = exec.bind(key, value);
            }
            
            // IO.print("\nCustomRepository.class: getAny() sql:" + sql); // DONT DELETE
            // IO.print("\nCustomRepository.class: getAny() binds: " + binds); // DONT DELETE

            return exec.map((row, _) -> mapConversion.rowToMap(row)).all();

        } catch (JsonProcessingException | IllegalArgumentException | IllegalStateException e) {
            IO.print("\nCustomRepository.java > getAny()" + e);
            return Flux.error(e);
        }
    }

    private String buildResults(String table, List<Object> columnsObject) {
        // If empty or contains a wildcard, select all columns
        if (columnsObject == null || columnsObject.isEmpty() || 
            (columnsObject.size() == 1 && "*".equals(columnsObject.get(0)))) {
            return "to_jsonb(" + q(table) + ")";  // Returns all columns as JSON
        }

        return "jsonb_build_object(" +
            columnsObject.stream()
                .map(columnObject -> {
            switch (columnObject) {
                case String regular_column -> {
                    // <key>, table.column
                    return "'" + regular_column + "', " + q(table) + "." + q(regular_column);
                }
                case Map<?, ?> jsonColumnMap -> {
                    Map.Entry<?, ?> jsonColumnEntry = jsonColumnMap.entrySet().iterator().next();
                    String jsonColumn = jsonColumnEntry.getKey().toString();
                    Object jsonObject = jsonColumnEntry.getValue();
                    
                switch (jsonObject) {
                    case List<?> nestedKeyList -> {
                        return "'" + jsonColumn + "', jsonb_build_object(" +
                                nestedKeyList.stream()
                                        .map(nestedKey -> apply(nestedKey, q(table) + "." + jsonColumn))
                                        .collect(Collectors.joining(", ")) +
                                ")";
                    }
                    case String regular_key -> {
                        return "'" + jsonColumn + "', jsonb_build_object(" +
                                "'" + regular_key + "', " + q(table) + "." + q(jsonColumn) + " ->> '" + regular_key + "'" +
                                ")";
                    }
                    default -> {
                        return "'" + jsonColumn + "', " + q(table) + "." + q(jsonColumn);
                    }
                }
                }
                default -> {
                    return "";
                }
            }
                })
                .collect(Collectors.joining(", ")) +
            ")";
    }
        
    private String apply(Object nestedObjects, String jsonColumn) {
        switch (nestedObjects) {
            case String regular_key -> {
                // 'key', jsonColumn ->> 'key'
                return "'" + regular_key + "', " + jsonColumn + " ->> '" + regular_key + "'";
            }
            case Map<?, ?> nestedMap -> {
                Map.Entry<?, ?> nestedEntry = nestedMap.entrySet().iterator().next();
                String nestedColumn = nestedEntry.getKey().toString();
                Object nestedObjectRecurse = nestedEntry.getValue();
                if (nestedObjectRecurse instanceof List<?> nestedObjectList) {
                    return "'" + nestedColumn + "', jsonb_build_object(" +
                            nestedObjectList.stream()
                                    .map(nestedObject -> apply(nestedObject, jsonColumn + " -> '" + nestedColumn + "'"))
                                    .collect(Collectors.joining(", ")) +
                            ")";
                } else {
                    throw new IllegalStateException("\nCustomRepository.java > apply() > Illegal format for nested Json: " + nestedObjects);
                }
            }
            default -> {
            }
        }
        return "";
    }

    private String buildJoins(Map<String, Object> associationsMap, String primaryTable, Boolean isInternalRequest) {
    StringBuilder joins = new StringBuilder();
    Set<String> joined = new HashSet<>();

    processJoinsRecursively(primaryTable, associationsMap, joins, joined, isInternalRequest);
    return joins.toString();
}
 
    private void processJoinsRecursively(
            String parentTable,
            Map<String, Object> associationsMap,
            StringBuilder joins,
            Set<String> joined,
            Boolean isInternalRequest
    ) {
        Map<String, Object> tableObj = objectMapper.convertValue(
                associationsMap.get(parentTable),
                new TypeReference<Map<String, Object>>() {}
        );
        if (tableObj == null) return;

        List<String> relations = objectMapper.convertValue(
                tableObj.get("relations"),
                new TypeReference<List<String>>() {}
        );
        List<String> pairingIds = objectMapper.convertValue(
                tableObj.get("pairingIds"),
                new TypeReference<List<String>>() {}
        );

        if (relations == null || pairingIds == null || pairingIds.size() != 2)
            return;

        String parentKey = pairingIds.get(0);
        String childKey  = pairingIds.get(1);

        for (String childTable : relations) {
            if (joined.contains(childTable)) continue;

            String sqlChildTable = isInternalRequest
                    ? childTable
                    : childTable + "SafeView";

            String left = parentKey.contains(".")
                ? Arrays.stream(parentKey.split("\\."))
                    .map(CustomRepository::q)
                    .collect(Collectors.joining("."))
                : q(parentTable) + "." + q(parentKey);

            String right = childKey.contains(".")
                ? Arrays.stream(childKey.split("\\."))
                    .map(CustomRepository::q)
                    .collect(Collectors.joining("."))
                : q(sqlChildTable) + "." + q(childKey);


            joins.append(" LEFT JOIN ")
                .append(q(sqlChildTable))
                .append(" ON ")
                .append(left)
                .append(" = ")
                .append(right);

            joined.add(childTable);

            processJoinsRecursively(childTable, associationsMap, joins, joined, isInternalRequest);
        }
    }

    private String paramName(String table, String column, String suffix) {
        // Replace anything that's not alphanumeric or underscore with "_"
        String base = table + "_" + column.replaceAll("[^a-zA-Z0-9_]", "_");
        if (suffix != null && !suffix.isBlank()) {
            base += "_" + suffix;
        }
        return base;
    }

    private String buildFilters(Object node, Map<String, Object> binds) throws JsonProcessingException {

        if (!(node instanceof Map<?, ?> map)) {
            throw new IllegalStateException("Filter node must be a Map, got: " + node);
        }

        // ✅ GROUP NODE
        if (map.containsKey("operator") && map.containsKey("conditions")) {

            String operator = map.get("operator").toString();
            Object children = map.get("conditions");

            if (!(children instanceof List<?> conditions)) {
                throw new IllegalStateException("'conditions' must be a list");
            }

            List<String> parts = new ArrayList<>();

            for (Object child : conditions) {
                parts.add(buildFilters(child, binds));
            }

            return "(" + String.join(" " + operator + " ", parts) + ")";
        }

        // ✅ LEAF NODE — must contain ONE column key
        if (map.size() == 1) {
            String key = map.keySet().iterator().next().toString();

            // ensure valid leaf format "table.column"
            if (!key.contains(".")) {
                throw new IllegalStateException("Invalid leaf filter key: " + key +
                        " — expected format: table.column");
            }

            return buildLeafCondition(map, binds);
        }

        // ❌ Anything else is illegal
        throw new IllegalStateException("Invalid filter format: " + map);
    }

    private String buildLeafCondition(Map<?, ?> leaf, Map<String, Object> binds) throws JsonProcessingException {

    // Get column key
    Map.Entry<?, ?> entry = leaf.entrySet().iterator().next();
    String fullColumn = entry.getKey().toString();

    // Safe JSON mapping
    Map<String, Object> columnMap = objectMapper.convertValue(
            entry.getValue(),
            new TypeReference<Map<String, Object>>() {}
    );

    // Extract parts
    String[] parts = fullColumn.split("\\.");
    String table = parts[0];
    String column = String.join(".", Arrays.copyOfRange(parts, 1, parts.length));

    String expr = resolveColumnExpression(table, column);

    Object value = columnMap.get("actualValue");
    Object typeObj = columnMap.get("actualType");

    // Safe cast
    if (!(typeObj instanceof String type)) {
        throw new IllegalStateException("actualType must be a string for: " + fullColumn);
    }

    String filter = type.split("::")[0];
    String cast = type.contains("::") ? type.substring(type.indexOf("::")) : "";

    String param = paramName(table, column, null);

    switch (filter) {

        case "exact" -> {
            binds.put(param, value);
            return cast.isEmpty()
                ? expr + " = :" + param
                : "(" + expr + ")" + cast + " = :" + param;
        }

        case "ilike" -> {
            binds.put(param, "%" + value + "%");
            return expr + " ILIKE :" + param;
        }

        case "range" -> {

            if (!(value instanceof List<?> range) || range.size() != 2) {
                throw new IllegalStateException("Range filter requires 2 values: " + fullColumn);
            }

            String from = paramName(table, column, "from");
            String to   = paramName(table, column, "to");

            binds.put(from, range.get(0));
            binds.put(to, range.get(1));

            return expr + " BETWEEN :" + from + cast + " AND :" + to + cast;
        }

        default -> throw new IllegalStateException("Unknown filter type: " + filter);
    }
}

    private String buildSearch(
            String search,
            Map<String, List<String>> searchFieldsMap,
             Map<String, Object> binds
    ) throws JsonProcessingException {

        List<String> whereClauses = new ArrayList<>();

        if (search != null && !search.isBlank() && !searchFieldsMap.isEmpty()) {
            List<String> tsQuery = new ArrayList<>();
            for (var tableEntry : searchFieldsMap.entrySet()) {
                String table = tableEntry.getKey();
                List<String> fields = tableEntry.getValue();
                for (String field : fields) {
                    String expr = resolveColumnExpression(table, field);
                    //tsQuery.add("to_tsvector('english', coalesce(" + expr + ",'')) @@ plainto_tsquery('english', :search)");
                    tsQuery.add("to_tsvector('english', coalesce(" + expr + ",'')) @@ to_tsquery('english', :search || ':*')");

                }
            }
            if (!tsQuery.isEmpty()) {
                whereClauses.add("(" + String.join(" OR ", tsQuery) + ")");
                binds.put("search", search);
            } else throw new IllegalStateException("Columns not specified for search query: " + search);
        }

        return whereClauses.isEmpty() ? "" : " AND " + String.join(" AND ", whereClauses);
    }

    private String buildSort(
            Map<String, List<Map<String, String>>> sortMap,
            /*Map<String, Object> binds,*/
            String search,
            Map<String, List<String>> searchFieldsMap
    ) {
        List<String> orderParts = new ArrayList<>();

        // --- 1. Add full-text search ranking if search is present ---
        if (search != null && !search.isBlank() && searchFieldsMap != null && !searchFieldsMap.isEmpty()) {
            List<String> tsVectors = searchFieldsMap.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(f -> "to_tsvector('english', coalesce(" + resolveColumnExpression(e.getKey(), f) + ",''))"))
                .toList();

            if (!tsVectors.isEmpty()) {
                String combinedTsVector = String.join(" || ", tsVectors);
                orderParts.add("ts_rank(" + combinedTsVector + ", to_tsquery('english', :search || ':*')) DESC");
            }
        }

        if (sortMap != null) {
            for (var tableEntry : sortMap.entrySet()) {
                String table = tableEntry.getKey();
                List<Map<String, String>> sortList = tableEntry.getValue();
                for (Map<String, String> details : sortList) {
                    String column = details.get("column");
                    String direction = details.getOrDefault("direction", "asc");
                    String expr = resolveColumnExpression(table, column);
                    orderParts.add(expr + " " + direction);
                }
            }
        }

        if (!orderParts.isEmpty()) {
            return " ORDER BY " + String.join(", ", orderParts);
        } else {
            return "";
        }
    }

    private String resolveColumnExpression(String table, String column) {
        return resolveColumnExpression(table, column, null);
    }

/*    
    private String resolveColumnExpression(String table, String column, String typeHint) {

        String[] parts = column.split("\\.");
        StringBuilder expr = new StringBuilder(table);

        // ✅ Only convert if camelCase
        String root = parts[0];
        String rootColumn = root.contains("_") ? root : camelToSnake(root);
        expr.append(".").append(rootColumn);

        // ✅ JSONB traversal unchanged
        for (int i = 1; i < parts.length; i++) {
            boolean last = (i == parts.length - 1);
            if (last) {
                expr.append(" ->> '").append(parts[i]).append("'");
            } else {
                expr.append(" -> '").append(parts[i]).append("'");
            }
        }

        String sqlExpr = expr.toString();

        // ✅ Preserve original CAST SAFETY
        if (typeHint != null) {
            if (typeHint.endsWith("::boolean")) {
                sqlExpr = "(" + sqlExpr + ")::boolean";
            } else if (typeHint.endsWith("::bigint")) {
                sqlExpr = "(" + sqlExpr + ")::bigint";
            } else if (typeHint.endsWith("::int")) {
                sqlExpr = "(" + sqlExpr + ")::int";
            } else if (typeHint.endsWith("::timestamptz")) {
                sqlExpr = "(" + sqlExpr + ")::timestamptz";
            } else if (typeHint.endsWith("::numeric")) {
                sqlExpr = "(" + sqlExpr + ")::numeric";
            }
        }

        return sqlExpr;
    }
*/

    private String resolveColumnExpression(String table, String column, String typeHint) {

        String[] parts = column.split("\\.");
        StringBuilder expr = new StringBuilder(q(table))
                .append(".")
                .append(q(parts[0]));

        // JSONB traversal unchanged
        for (int i = 1; i < parts.length; i++) {
            boolean last = (i == parts.length - 1);
            expr.append(last
                ? " ->> '" + parts[i] + "'"
                : " -> '" + parts[i] + "'");
        }

        return typeHint == null
            ? expr.toString()
            : "(" + expr + ")" + typeHint;
    }

}
