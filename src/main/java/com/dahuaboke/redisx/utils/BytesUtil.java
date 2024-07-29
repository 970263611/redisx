package com.dahuaboke.redisx.utils;

public class BytesUtil {

    public static int byteArrayToInt(byte[] byteArray) {
        if (byteArray.length < 1 || byteArray.length > 4) {
            return -1;
        }
        int result = 0;
        for (int i = 0; i < byteArray.length; i++) {
            result = result << 8 | (byteArray[i] & 0xFF);
        }
        return result;
    }

    public static int byteArrayToIntLE(byte[] byteArray) {
        if (byteArray.length < 1 || byteArray.length > 4) {
            return -1;
        }
        int result = 0;
        for (int i = byteArray.length - 1; i >= 0; i--) {
            result = result << 8 | (byteArray[i] & 0xFF);
        }
        return result;
    }

}
