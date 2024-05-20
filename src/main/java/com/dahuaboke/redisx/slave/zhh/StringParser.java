package com.dahuaboke.redisx.slave.zhh;

import io.netty.buffer.ByteBuf;
import com.dahuaboke.redisx.slave.zhh.LengthParser.Len;
import static com.dahuaboke.redisx.Constant.*;

/**
 * @Desc: 字符串解析器
 * @Author：zhh
 * @Date：2024/5/20 14:01
 */
public class StringParser {

    LengthParser length = new LengthParser();
    public byte[] parseString(ByteBuf byteBuf){
        Len lenObj = length.parseLength(byteBuf);
        long len = (int) lenObj.len;
        boolean isEncoded = lenObj.encoded;
        //特殊格式编码,整数/压缩字符串
        if (isEncoded) {
            switch ((int) len) {
                case RDB_ENC_INT8:
                case RDB_ENC_INT16:
                case RDB_ENC_INT32:
                    return parseIntegerObject((int) len, byteBuf);
                case RDB_ENC_LZF:
                    return parseLzfStringObject(byteBuf);
                default:
                    throw new AssertionError("unknown RdbParser encoding type:" + len);
            }
        }
        byte[] value = new byte[(int) len];
        byteBuf.readBytes(value);
        return value;
    }

    private byte[] parseIntegerObject(int enctype, ByteBuf byteBuf){
        byte[] value;
        switch (enctype) {
            case RDB_ENC_INT8:
                value = new byte[1];
                byteBuf.readBytes(value);
                break;
            case RDB_ENC_INT16:
                value = new byte[2];
                byteBuf.readBytes(value);
                break;
            case RDB_ENC_INT32:
                value = new byte[4];
                byteBuf.readBytes(value);
                break;
            default:
                value = new byte[]{0x00};
                break;
        }
        return String.valueOf(verseBigEndian(value)).getBytes();
    }

    public int verseBigEndian(byte[] bytes) {
        int r = 0;
        int length = bytes.length;
        for (int i = 0; i < length; ++i) {
            final int v = bytes[i] & 0xFF;
            r |= (v << (i << 3));
        }
        int c;
        return r << (c = (4 - length << 3)) >> c;
    }

    private byte[] parseLzfStringObject(ByteBuf byteBuf) {
        //TODO 压缩字符串解析
        return null;
    }
}
