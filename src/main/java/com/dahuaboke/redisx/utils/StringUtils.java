package com.dahuaboke.redisx.utils;

import java.lang.reflect.Method;

public class StringUtils {

    /**
     * 字符串是否为空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 字符串是否非空
     *
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return str != null && str.length() > 0;
    }

    /**
     * 首字母转大写
     *
     * @param str
     * @return
     */
    public static String upperFirstLetter(String str) {
        if (isEmpty(str)) {
            return str;
        }
        StringBuilder sb = new StringBuilder(str);
        sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
        return sb.toString();
    }

    /**
     * 数据类型转换
     *
     * @param obj  数据，必须是基本数据类型或字符串类型
     * @param type 要转换的类型
     * @return
     */
    static Object changePrimitive(Object obj, Class<?> type) {
        if (obj instanceof String) {
            if (type == String.class) {
                return obj;
            }
            return convert((String) obj, type);
        } else if (isPrimitive(obj)) {
            obj = primitiveToString(obj);
            return convert((String) obj, type);
        } else {
            return obj;
        }
    }

    /**
     * 基本数据类型转String
     *
     * @param obj
     * @return
     */
    static String primitiveToString(Object obj) {
        if (isPrimitive(obj)) {
            return String.valueOf(obj);
        } else {
            return null;
        }
    }

    /**
     * 判断是否是基本数据类型或者其包装类
     *
     * @param obj
     * @return
     */
    public static boolean isPrimitive(Object obj) {
        if (obj.getClass().isPrimitive()) {
            return true;
        }
        if (obj instanceof Integer
                || obj instanceof Long
                || obj instanceof Double
                || obj instanceof Float
                || obj instanceof Boolean
                || obj instanceof Character
                || obj instanceof Byte
                || obj instanceof Short) {
            return true;
        }
        return false;
    }

    /**
     * 字符串转基本数据类ing
     *
     * @param value 字符串值
     * @param type  要转的基本类型
     * @param <T>
     * @return
     */
    public static <T> T convert(String value, Class<T> type) {
        try {
            // 对于基本类型，需要使用包装类
            Class<?> wrapperClass = getWrapperClass(type);
            if (wrapperClass != null) {
                Method method = wrapperClass.getMethod("valueOf", String.class);
                return (T) method.invoke(null, value);
            }

            // 对于其他类型，直接使用构造函数
            return type.getConstructor(String.class).newInstance(value);
        } catch (Exception e) {
            throw new RuntimeException("转换失败", e);
        }
    }

    private static Class<?> getWrapperClass(Class<?> type) {
        Class<?> clazz = null;
        switch (type.getName()) {
            case "int":
                clazz = Integer.class;
                break;
            case "byte":
                clazz = Byte.class;
                break;
            case "short":
                clazz = Short.class;
                break;
            case "long":
                clazz = Long.class;
                break;
            case "double":
                clazz = Double.class;
                break;
            case "float":
                clazz = Float.class;
                break;
            case "boolean":
                clazz = Boolean.class;
                break;
            case "char":
                clazz = Character.class;
                break;
            default:
                break;
        }
        return clazz;
    }

}
