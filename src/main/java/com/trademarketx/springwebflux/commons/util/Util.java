package com.trademarketx.springwebflux.commons.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;

public final class Util {

    private Util() {}

    public static String getTableName(Class<?> entityClass) {

        Table table = entityClass.getAnnotation(Table.class);
        if (table != null && !table.value().isBlank()) {
            return table.value();
        }

        // Remove Entity suffix
        String name = entityClass.getSimpleName().replaceAll("Entity$", "");

        // Split CamelCase into words
        String[] words = name.split("(?=[A-Z])");

        // Pluralize EACH word
        for (int i = 0; i < words.length; i++) {
            String w = words[i];

            if (w.endsWith("y")) {
                words[i] = w.substring(0, w.length() - 1) + "ies";
            } else if (!w.endsWith("s")) {
                words[i] = w + "s";
            }
        }

        // Rejoin
        String pluralName = String.join("", words);

        // Lowercase first character only
        return pluralName.substring(0, 1).toLowerCase() + pluralName.substring(1);
    }

    public static boolean isTransient(Field field) {
        return Modifier.isTransient(field.getModifiers()) || field.isAnnotationPresent(Transient.class);
    }

    public static boolean isJsonType(Class<?> clazz) {
        if (clazz.isPrimitive() || clazz.isEnum()) return false;
        Package pkg = clazz.getPackage();
        return !(pkg != null && pkg.getName().startsWith("java."));
    }

    public static boolean isPrimitiveOrSimple(Class<?> clazz) {
        return Set.of(
            Boolean.class, Byte.class, Short.class, Integer.class, Long.class,
            Float.class, Double.class, Character.class, String.class,
            BigInteger.class, BigDecimal.class,
            Instant.class, LocalDateTime.class, LocalDate.class,
            LocalTime.class, ZonedDateTime.class,
            UUID.class
        ).contains(clazz);
    }

    public static String getBaseUrl(ServerWebExchange exchange) {
        ServerHttpRequest req = exchange.getRequest();
        String scheme = req.getHeaders().getFirst("X-Forwarded-Proto");
        if (scheme == null) scheme = req.getURI().getScheme();

        String host = req.getHeaders().getFirst("X-Forwarded-Host");
        if (host == null) host = req.getURI().getHost();

        String portHeader = req.getHeaders().getFirst("X-Forwarded-Port");
        int port = portHeader != null ? Integer.parseInt(portHeader) : req.getURI().getPort();

        if ((scheme.equals("http") && port != 80) || (scheme.equals("https") && port != 443)) {
            host += ":" + port;
        }
        return scheme + "://" + host;
    }
}
