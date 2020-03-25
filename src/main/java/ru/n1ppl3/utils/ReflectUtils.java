package ru.n1ppl3.utils;

import org.springframework.lang.NonNull;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;


public abstract class ReflectUtils {

    public static Object getObjectFieldValue(@NonNull Object object, String fieldName) {
        Field field = ReflectionUtils.findField(object.getClass(), fieldName);
        if (field == null) {
            throw new RuntimeException("couldn't find " + fieldName + " field in " + object.getClass());
        }
        ReflectionUtils.makeAccessible(field);
        return ReflectionUtils.getField(field, object);
    }
}
