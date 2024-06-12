package com.dahuaboke.redisx.from.rdb.base;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import com.dahuaboke.redisx.from.rdb.base.LengthParser.Len;
import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.Constant.*;

/**
 * @Desc: 字符串解析器
 * @Author：zhh
 * @Date：2024/5/20 14:01
 */
public class StringParser implements Parser {

    public byte[] parse(ByteBuf byteBuf){
        Len lenObj = ParserManager.LENGTH.parse(byteBuf);
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
        //压缩后长度
        int compressedLen = (int)ParserManager.LENGTH.parse(byteBuf).len;
        //压缩前长度
        int len = (int)ParserManager.LENGTH.parse(byteBuf).len;
        //压缩后字节数组
        byte[] src = new byte[compressedLen];
        byteBuf.readBytes(src);
        //压缩前字节数组
        byte[] dest = new byte[len];
        Lzf.expand(src,dest);
        return dest;
    }

    final static class Lzf {
        private static int MAX_LITERAL = 32;

        static void expand(byte[] src, byte[] dest) {
            int srcPos = 0;
            int destPos = 0;
            do {
                int ctrl = src[srcPos++] & 0xff;
                if (ctrl < MAX_LITERAL) {
                    ctrl++;
                    System.arraycopy(src, srcPos, dest, destPos, ctrl);
                    destPos += ctrl;
                    srcPos += ctrl;
                } else {
                    int len = ctrl >> 5;
                    if (len == 7) {
                        len += src[srcPos++] & 0xff;
                    }
                    len += 2;
                    ctrl = -((ctrl & 0x1f) << 8) - 1;
                    ctrl -= src[srcPos++] & 0xff;
                    ctrl += destPos;
                    for (int i = 0; i < len; i++) {
                        dest[destPos++] = dest[ctrl++];
                    }
                }
            } while (destPos < dest.length);
        }
    }
}
