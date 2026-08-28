package com.trademarketx.springwebflux.commons;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class CustomValidation<T> {

    private final Validator validator;

    public CustomValidation(Validator validator) {
        this.validator = validator;
    }

    public Map<String, List<String>> validateAll(T entity) {
        Map<String, List<String>> errors = new HashMap<>();
        Set<ConstraintViolation<T>> violations = validator.validate(entity);

        for (ConstraintViolation<T> v : violations) {
            errors.computeIfAbsent(v.getPropertyPath().toString(), k -> new ArrayList<>())
                  .add(v.getMessage());
        }
        return errors;
    }

    public Map<String, List<String>> validate(Class<?> entityClass, Map<String, Object> updates) {

        Map<String, List<String>> errors = new LinkedHashMap<>();
        Map<String, Field> fields = new HashMap<>();

        for (Field f : entityClass.getDeclaredFields()) {
            f.setAccessible(true);
            fields.put(f.getName(), f);
        }

        updates.forEach((key, value) -> {
            Field field = fields.get(key);
            if (field == null) {
                errors.computeIfAbsent(key, k -> new ArrayList<>())
                      .add("Unknown field");
                return;
            }

            Set<? extends ConstraintViolation<?>> violations = validator.validateValue(entityClass, field.getName(), value);

            for (ConstraintViolation<?> v : violations) {
                errors.computeIfAbsent(key, k -> new ArrayList<>())
                      .add(v.getMessage());
            }
        });

        return errors;
    }
}
