package com.dahuaboke.redisx.utils;

import com.dahuaboke.redisx.annotation.FieldOrm;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

public class FieldOrmUtil {

    public static <T> void MapToBean(Map<String, Object> map , T bean) {
        Class<?> clazz = bean.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for(Field field:declaredFields){
            FieldOrm FieldOrm = field.getAnnotation(FieldOrm.class);
            if(FieldOrm == null){
                continue;
            }
            //必须有映射名称
            if(StringUtils.isEmpty(FieldOrm.value())){
                throw new IllegalArgumentException("config map failed,name is null : " + field.getName());
            }
            //参数值获取及类型转换
            Object val = map.get(FieldOrm.value());
            if(val == null){
                if(FieldOrm.required()){
                    throw new IllegalArgumentException("config map failed,param is required : " + FieldOrm.value());
                }
                if(StringUtils.isNotEmpty(FieldOrm.defaultValue())){
                    val = FieldOrm.defaultValue();
                }
            }
            if(val == null){
                continue;
            }
            if(val.getClass() != field.getType()){
                val = StringUtils.changePrimitive(val,field.getType());
            }
            //获取set方法
            Method method = null;
            try {
                method = clazz.getMethod("set" + StringUtils.upperFirstLetter(field.getName()),field.getType());
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("config map failed,field not find set method : " + field.getName());
            }
            //设置值
            try {
                method.invoke(bean,val);
            } catch (Exception e) {
                throw new IllegalArgumentException("config map failed,set value error : " + field.getName());
            }
        }
    }



}
