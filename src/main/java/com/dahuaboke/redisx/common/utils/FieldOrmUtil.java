package com.dahuaboke.redisx.common.utils;

import com.dahuaboke.redisx.common.annotation.FieldOrm;
import com.dahuaboke.redisx.common.annotation.FieldOrmCheck;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class FieldOrmUtil {

    public static <T> void MapToBean(Map<String, Object> map, T bean) {
        Class<?> clazz = bean.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            FieldOrm fieldOrm = field.getAnnotation(FieldOrm.class);
            if (fieldOrm == null) {
                continue;
            }
            //必须有映射名称
            if (StringUtils.isEmpty(fieldOrm.value())) {
                throw new IllegalArgumentException("Config map failed, name is null : " + field.getName());
            }
            //参数值获取及类型转换
            Object val = map.get(fieldOrm.value());
            if (val == null) {
                if (fieldOrm.required()) {
                    throw new IllegalArgumentException("Config map failed, param is required : " + fieldOrm.value());
                }
                if (StringUtils.isNotEmpty(fieldOrm.defaultValue())) {
                    val = fieldOrm.defaultValue();
                }
            }
            if (val == null) {
                continue;
            }
            Class<?> setType = field.getType();
            if (fieldOrm.setType() != void.class) {
                setType = fieldOrm.setType();
            }
            if (val.getClass() != setType) {
                val = StringUtils.changePrimitive(val, field.getType());
            }
            //获取set方法
            Method method;
            try {
                method = clazz.getMethod("set" + StringUtils.upperFirstLetter(field.getName()), setType);
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Config map failed, field not find set method : " + field.getName());
            }
            //设置值
            try {
                method.invoke(bean, val);
            } catch (Exception e) {
                throw new IllegalArgumentException("Config map failed, set value error : " + field.getName());
            }
        }
        if (FieldOrmCheck.class.isAssignableFrom(clazz)) {
            Method method = null;
            try {
                method = clazz.getMethod("check");
            } catch (NoSuchMethodException e) {
            }
            if (method != null) {
                try {
                    method.invoke(bean);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
