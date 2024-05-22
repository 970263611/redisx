package com.dahuaboke.redisx.slave.rdb.base;

import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.Constant.*;

/**
 * @Desc: length解析器
 * @Author：zhh
 * @Date：2024/5/20 13:54
 */
public class LengthParser implements Parser{

    public Len parse(ByteBuf byteBuf){
        boolean isencoded = false;
        int rawByte = byteBuf.readByte() & 0xFF;
        int type = (rawByte & 0xC0) >> 6;
        long value;
        if (type == RDB_ENCVAL) {
            //11 代表特殊格式编码
            isencoded = true;
            value = rawByte & 0x3F;
        } else if (type == RDB_6BITLEN) {
            value = rawByte & 0x3F;
        } else if (type == RDB_14BITLEN) {
            value = ((rawByte & 0x3F) << 8) | byteBuf.readByte() & 0xFF;
        } else if (rawByte == RDB_32BITLEN) {
            //大端格式
            value = byteBuf.readInt() & 0xFFFFFFFFL;
        } else if (rawByte == RDB_64BITLEN) {
            //大端格式
            value = byteBuf.readLong();
        } else {
            throw new AssertionError("unexpected len-type:" + type);
        }
        return new Len(value, isencoded);
    }

    public  class Len {
        public final long len;

        /**
         * true为特殊编码格式
         */
        public final boolean encoded;

        private Len(long len, boolean encoded) {
            this.len = len;
            this.encoded = encoded;
        }
    }
}
