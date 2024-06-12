package com.dahuaboke.redisx.from.rdb.base;

import com.dahuaboke.redisx.from.rdb.ParserManager;
import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.from.rdb.RdbConstants.*;

/**
 * @author cdl
 * @Date：2024/5/24 12:00
 */
public class SkipRdbParser {

    
    public void rdbLoadTime(ByteBuf byteBuf) {
        byteBuf.readInt();
    }
    
    public void rdbLoadMillisecondTime(ByteBuf byteBuf) {
        byteBuf.readLong();
    }

    public LengthParser.Len rdbLoadLen(ByteBuf byteBuf)  {
        return ParserManager.LENGTH.parse(byteBuf);
    }

    public void rdbLoadIntegerObject(ByteBuf byteBuf,int enctype) {
        switch (enctype) {
            case RDB_ENC_INT8:
                byteBuf.readByte();
                break;
            case RDB_ENC_INT16:
                byteBuf.readShort();
                break;
            case RDB_ENC_INT32:
                byteBuf.readShort();
                break;
            default:
                break;
        }
    }
    
    public void rdbLoadLzfStringObject(ByteBuf byteBuf) {
        long clen = rdbLoadLen(byteBuf).len;
        rdbLoadLen(byteBuf);
    }
    
    public void rdbGenericLoadStringObject(ByteBuf byteBuf) {
        LengthParser.Len lenObj = rdbLoadLen(byteBuf);
        long len = (int) lenObj.len;
        boolean isencoded = lenObj.encoded;
        if (isencoded) {
            switch ((int) len) {
                case RDB_ENC_INT8:
                case RDB_ENC_INT16:
                case RDB_ENC_INT32:
                    rdbLoadIntegerObject(byteBuf,(int) len);
                    return;
                case RDB_ENC_LZF:
                    rdbLoadLzfStringObject(byteBuf);
                    return;
                default:
                    throw new AssertionError("unknown RdbParser encoding type:" + len);
            }
        }
    }
    
    public void rdbLoadPlainStringObject(ByteBuf byteBuf) {
        rdbGenericLoadStringObject(byteBuf);
    }
    
    public void rdbLoadEncodedStringObject(ByteBuf byteBuf) {
        rdbGenericLoadStringObject(byteBuf);
    }

    
    public void rdbLoadBinaryDoubleValue(ByteBuf byteBuf) {
        byteBuf.readLong();
    }
    
    public float rdbLoadBinaryFloatValue(ByteBuf byteBuf) {
        return byteBuf.readFloat();
    }
    
    public void rdbLoadCheckModuleValue(ByteBuf byteBuf) {
        int opcode;
        while ((opcode = (int) rdbLoadLen(byteBuf).len) != RDB_MODULE_OPCODE_EOF) {
            if (opcode == RDB_MODULE_OPCODE_SINT || opcode == RDB_MODULE_OPCODE_UINT) {
                rdbLoadLen(byteBuf);
            } else if (opcode == RDB_MODULE_OPCODE_STRING) {
                rdbGenericLoadStringObject(byteBuf);
            } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
                rdbLoadBinaryFloatValue(byteBuf);
            } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
                rdbLoadBinaryDoubleValue(byteBuf);
            }
        }
    }
}
