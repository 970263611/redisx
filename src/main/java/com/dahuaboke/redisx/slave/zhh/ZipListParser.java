package com.dahuaboke.redisx.slave.zhh;

import io.netty.buffer.ByteBuf;
import java.util.LinkedList;
import java.util.List;

import static com.dahuaboke.redisx.Constant.*;

/**
 * @Desc: zipList解析器
 * @Author：zhh
 * @Date：2024/5/17 16:08
 */
public class ZipListParser {

    public List<byte[]> parseZipList(ByteBuf byteBuf) {
        List<byte[]> list = new LinkedList();
        //总字节长度
        int size = byteBuf.readIntLE();
        //尾部偏移量
        int tail = byteBuf.readIntLE();
        //元素个数
        short numElements = byteBuf.readShortLE();
        for (int i = 0; i < numElements; i++) {
            list.add(getZipListEntry(byteBuf));
        }
        //尾部结束符
        int end = byteBuf.readByte() & 0xFF;
        if (end != 255) {
            throw new AssertionError("zipList expect 255 but " + end);
        }
        return list;
    }

    private byte[] getZipListEntry(ByteBuf byteBuf) {
        //前一个entry的长度
        int prevlen = byteBuf.readByte() & 0xFF;
        if (prevlen >= 254) {
            prevlen = byteBuf.readIntLE();
        }
        //该标志指示entry是字符串还是整数
        int special = byteBuf.readByte() & 0xFF;
        //原始字节
        byte[] value;
        //entry是字符串
        switch (special >> 6) {
            case 0:
                int len = special & 0x3F;
                value = new byte[len];
                byteBuf.readBytes(value);
                return value;
            case 1:
                len = ((special & 0x3F) << 8) | byteBuf.readByte() & 0xFF;
                value = new byte[len];
                byteBuf.readBytes(value);
                return value;
            case 2:
                //大端格式
                len = byteBuf.readInt();
                value = new byte[len];
                byteBuf.readBytes(value);
                return value;
            default:
                break;
        }
        //entry是整数
        switch (special) {
            case ZIP_INT_8B:
                return String.valueOf(byteBuf.readByte() & 0xFF).getBytes();
            case ZIP_INT_16B:
                return String.valueOf(byteBuf.readShortLE()).getBytes();
            case ZIP_INT_24B:
                return String.valueOf(this.verseBigEndian(byteBuf,3)).getBytes();
            case ZIP_INT_32B:
                return String.valueOf(byteBuf.readIntLE()).getBytes();
            case ZIP_INT_64B:
                return String.valueOf(byteBuf.readLongLE()).getBytes();
            default:
                //6BIT
                return String.valueOf(special - 0xF1).getBytes();
        }
    }

    /**
     * litterEndian verse bigEndian
     * @param byteBuf
     * @param length
     * @return
     */
    public int verseBigEndian(ByteBuf byteBuf, int length){
        int r = 0;
        for (int i = 0; i < length; ++i) {
            final int v = byteBuf.readByte() & 0xFF;
            r |= (v << (i << 3));
        }
        int c;
        return r << (c = (4 - length << 3)) >> c;
    }
}
