package com.dahuaboke.redisx.from.rdb.base;

import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

/**
 * @Desc: listPack解析器
 * @Author：zhh
 * @Date：2024/5/17 14:26
 */
public class ListPackParser implements Parser {
    public List<byte[]> parse(ByteBuf byteBuf) {
        List<byte[]> list = new LinkedList();
        //tot-bytes总字节长度
        int size = byteBuf.readIntLE();
        //num-elements元素个数
        short numElements = byteBuf.readShortLE();
        for (int i = 0; i < numElements; i++) {
            list.add(getListPackEntry(byteBuf));
        }
        //尾部结束符
        int end = byteBuf.readByte() & 0xFF;
        if (end != 255) {
            throw new AssertionError("listPack expect 255 but " + end);
        }
        return list;
    }

    /**
     * |0xxxxxx|7 位无符号整数
     * |10xxxxxx|6 位无符号整数作为字符串长度。然后将字节读取为字符串。
     * |110xxxxx|xxxxxxxx|13 位有符号整数。
     * |1110xxxx|xxxxxxxx|长度不超过 4095 的字符串。
     * |11110001|xxxxxxxx|xxxxxxxx|接下来的 2 个字节为 16 位 int.
     * |11110010|xxxxxxxx|xxxxxxxx|xxxxxxxx|接下来的 3 个字节为 24 位 int。
     * |11110011|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|接下来的 4 个字节为 32 位 int.
     * |11110100|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|接下来的 8 个字节长度为 64 位。
     * |11110000|xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|接下来的 4 个字节作为字符串长度。然后将字节读取为字符串
     *
     * @param byteBuf
     * @return
     */
    public byte[] getListPackEntry(ByteBuf byteBuf) {
        //encoding-type 编码类型
        int encodingType = byteBuf.readByte() & 0xFF;
        //element-data 原始字节
        byte[] value;
        //element-tot-len=encoding-type + element-data的总长度,不包含自己的长度
        long elementTotLen;
        if ((encodingType & 0x80) == 0) {
            elementTotLen = 1;
            //取7 位无符号整数
            value = String.valueOf(encodingType & 0x7F).getBytes();
        } else if ((encodingType & 0xC0) == 0x80) { //取前两位判断是否是10
            int len = encodingType & 0x3F;
            elementTotLen = 1 + len;
            value = new byte[len];
            byteBuf.readBytes(value);
        } else if ((encodingType & 0xE0) == 0xC0) { //取前三位判断是否是110
            elementTotLen = 2;
            int next = byteBuf.readByte() & 0xFF;
            value = String.valueOf((((encodingType & 0x1F) << 8) | next) << 19 >> 19).getBytes();
        } else if ((encodingType & 0xFF) == 0xF1) {
            elementTotLen = 3;
            value = String.valueOf(byteBuf.readShortLE()).getBytes();
        } else if ((encodingType & 0xFF) == 0xF2) {
            elementTotLen = 4;
            value = String.valueOf(this.verseBigEndian(byteBuf, 3)).getBytes();
        } else if ((encodingType & 0xFF) == 0xF3) {
            elementTotLen = 5;
            value = String.valueOf(byteBuf.readIntLE()).getBytes();
        } else if ((encodingType & 0xFF) == 0xF4) {
            elementTotLen = 9;
            value = String.valueOf(byteBuf.readLongLE()).getBytes();
        } else if ((encodingType & 0xF0) == 0xE0) {
            int len = ((encodingType & 0x0F) << 8) | byteBuf.readByte() & 0xFF;
            elementTotLen = 2 + len;
            value = new byte[len];
            byteBuf.readBytes(value);
        } else if ((encodingType & 0xFF) == 0xF0) {
            int len = byteBuf.readIntLE();
            elementTotLen = 5 + len;
            value = new byte[len];
            byteBuf.readBytes(value);
        } else {
            throw new UnsupportedOperationException(String.valueOf(encodingType));
        }
        //长度不同,占用的字节不同,就是element-tot-len的字节
        if (elementTotLen <= 127) {
            byteBuf.skipBytes(1);
        } else if (elementTotLen < 16383) {
            byteBuf.skipBytes(2);
        } else if (elementTotLen < 2097151) {
            byteBuf.skipBytes(3);
        } else if (elementTotLen < 268435455) {
            byteBuf.skipBytes(4);
        } else {
            byteBuf.skipBytes(5);
        }
        return value;
    }

    /**
     * litterEndian verse bigEndian
     *
     * @param byteBuf
     * @param length
     * @return
     */
    public int verseBigEndian(ByteBuf byteBuf, int length) {
        int r = 0;
        for (int i = 0; i < length; ++i) {
            final int v = byteBuf.readByte() & 0xFF;
            r |= (v << (i << 3));
        }
        int c;
        return r << (c = (4 - length << 3)) >> c;
    }
}
