package com.dahuaboke.redisx.from.rdb;


/**
 * @Desc: RDB常量类
 * @Author：zhh
 * @Date：2024/5/20 14:01
 */
public class RdbConstants {

    public static final String START = "REDIS";

    public static final int RDB_OPCODE_AUX = 0xfa & 0xff;  //$string, $string; 250

    public static final int RDB_OPCODE_MODULE_AUX = 0xf7 & 0xff; //$module2; 247

    public static final int RDB_OPCODE_FUNCTION2 = 0xf5 & 0xff; //$function; 245

    public static final int RDB_OPCODE_SELECTDB = 0xfe & 0xff; //$length; 254

    public static final int RDB_OPCODE_RESIZEDB = 0xfb & 0xff; //$length, $length; 251

    public static final int RDB_OPCODE_EXPIRETIME = 0xfd & 0xff; //253

    public static final int RDB_OPCODE_IDLE = 0xf8 & 0xff;//248

    public static final int RDB_OPCODE_FREQ = 0xf9 & 0xff;//249

    public static final int RDB_OPCODE_EXPIRETIME_MS = 0xfc & 0xff;//252

    public static final int RDB_OPCODE_SLOT_INFO = 0xf4 & 0xff;//244

    public static final int RDB_OPCODE_FUNCTION_PRE_GA = 0xf6 & 0xff;//246

    public static final int EOF = 0xff & 0xff;//255


    /**
     * Module serialized values sub opcodes
     */
    public static final int RDB_MODULE_OPCODE_EOF = 0; /* End of module value. */
    public static final int RDB_MODULE_OPCODE_SINT = 1; /* Signed integer. */
    public static final int RDB_MODULE_OPCODE_UINT = 2; /* Unsigned integer. */
    public static final int RDB_MODULE_OPCODE_FLOAT = 3; /* Float. */
    public static final int RDB_MODULE_OPCODE_DOUBLE = 4; /* Double. */
    public static final int RDB_MODULE_OPCODE_STRING = 5; /* String. */

    /**
     * string encoding
     */
    public static final int RDB_ENC_INT8 = 0;
    public static final int RDB_ENC_INT16 = 1;
    public static final int RDB_ENC_INT32 = 2;
    public static final int RDB_ENC_LZF = 3;

}
