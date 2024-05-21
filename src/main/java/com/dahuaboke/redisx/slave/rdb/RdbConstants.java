package com.dahuaboke.redisx.slave.rdb;


/**
 * @Desc: RDB常量类
 * @Author：zhh
 * @Date：2024/5/20 14:01
 */
public class RdbConstants {

    public static final String START = "REDIS";

    public static final int AUX = 0xfa & 0xff;  //$string, $string;

    public static final int MODULE_AUX = 0xf7 & 0xff; //$module2;

    public static final int FUNCTION = 0xf5 & 0xff; //$function;

    public static final int DBSELECT = 0xfe & 0xff; //$length;

    public static final int DBRESIZE = 0xfb & 0xff; //$length, $length;

    public static final int EXPIRED_FD = 0xfd & 0xff;

    public static final int EXPIRED_FC = 0xfc & 0xff;

    public static final int IDLE = 0xf8 & 0xff;

    public static final int FREQ = 0xf9 & 0xff;

    public static final int EOF = 0xff & 0xff;


}
