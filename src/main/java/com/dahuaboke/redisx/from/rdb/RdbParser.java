package com.dahuaboke.redisx.from.rdb;

import com.dahuaboke.redisx.from.rdb.base.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * @Desc: RDB解析类
 * @Author：cdl
 * @Date：2024/5/20 14:01
 */
public class RdbParser {

    private static final Logger logger = LoggerFactory.getLogger(RdbParser.class);

    private RdbInfo rdbInfo;

    private ByteBuf byteBuf;

    public RdbParser(ByteBuf byteBuf) {
        //---  为了方便观察Rdb内容状况，打印前后各9位 跟业务无关---
        ByteBuf start9 = Unpooled.buffer();
        ByteBuf end9 = Unpooled.buffer();
        if (byteBuf.writerIndex() >= 9) {
            start9 = byteBuf.slice(0, 9);
            end9 = byteBuf.slice(byteBuf.writerIndex() - 9, 9);
        }
        logger.info("Rdb prase start,rdbBuf length is = " + byteBuf.readableBytes()
                + ",\r\n the start 9 byte is \r\n" + ByteBufUtil.prettyHexDump(start9)
                + "',\r\n the end 9 byte is \r\n" + ByteBufUtil.prettyHexDump(end9));
        //---  为了方便观察Rdb内容状况，打印前后各9位 跟业务无关---
        this.rdbInfo = new RdbInfo();
        if (!RdbConstants.START.equals(byteBuf.readBytes(5).toString(Charset.defaultCharset()))) {
            logger.error("rdb file format error");
            throw new RuntimeException("rdb file format error");
        }
        rdbHeader().setVer(byteBuf.readBytes(4).toString(Charset.defaultCharset()));
        this.byteBuf = byteBuf;
    }


    /**
     * 文件解析
     */
    public void parse() {
        clear();
        while (true) {
            int b = byteBuf.readByte() & 0xff;
            switch (b) {
                case RdbConstants.EOF:
                    rdbInfo.setEnd(true);
                    rdbInfo.setRdbData(null);
                    return;
                //头部分
                case RdbConstants.AUX:
                    auxParse();
                    return;
                case RdbConstants.MODULE_AUX:
                    moduleParse();
                    return;
                case RdbConstants.FUNCTION:
                    functionParse();
                    rdbInfo.setFunctionReady(true);
                    return;
                //数据部分
                case RdbConstants.DBSELECT:
                    rdbData().setSelectDB(ParserManager.LENGTH.parse(byteBuf).len);
                    rdbData().setDataNum(1);
                    break;
                case RdbConstants.DBRESIZE:
                    rdbData().setDataCount(ParserManager.LENGTH.parse(byteBuf).len);
                    rdbData().setTtlCount(ParserManager.LENGTH.parse(byteBuf).len);
                    break;
                case RdbConstants.EXPIRED_FC:
                    rdbData().setExpiredType(ExpiredType.MS);
                    rdbData().setExpireTime(byteBuf.readLongLE());
                    break;
                case RdbConstants.EXPIRED_FD:
                    rdbData().setExpiredType(ExpiredType.SECOND);
                    rdbData().setExpireTime(byteBuf.readIntLE());
                    break;
                case RdbConstants.RDB_OPCODE_IDLE:
                    rdbData().setEvictType(EvictType.LRU);
                    rdbData().setEvictValue(ParserManager.LENGTH.parse(byteBuf).len);
                    break;
                case RdbConstants.RDB_OPCODE_FREQ:
                    rdbData().setEvictType(EvictType.LFU);
                    rdbData().setEvictValue((long) (byteBuf.readByte() & 0xff));
                    break;
                default:
                    rdbData().setRdbType(b);
                    rdbData().setKey(ParserManager.STRING_00.parse(byteBuf));
                    Parser parser = ParserManager.getParser(b);
                    if (parser != null) {
                        rdbData().setValue(parser.parse(byteBuf));
                    }
                    rdbData().setDataNum(rdbData().getDataNum() + 1);
                    rdbInfo.setDataReady(true);
                    return;
            }
        }
    }

    private void auxParse() {
        String key = new String(ParserManager.STRING_00.parse(byteBuf));
        String value = new String(ParserManager.STRING_00.parse(byteBuf));
        switch (key) {
            case "redis-ver":
                rdbHeader().setRedisVer(value);
                return;
            case "redis-bits":
                rdbHeader().setRedisBits(value);
                return;
            case "ctime":
                rdbHeader().setCtime(value);
                return;
            case "used-mem":
                rdbHeader().setUsedMem(value);
                return;
            case "repl-stream-db":
                rdbHeader().setReplStreamDb(value);
                return;
            case "repl-id":
                rdbHeader().setReplId(value);
                return;
            case "repl-offset":
                rdbHeader().setReplOffset(value);
                return;
            case "aof-base":
                rdbHeader().setAofBase(value);
                return;
            default:
                return;
        }
    }

    private void moduleParse() {
        ParserManager.SKIP.rdbLoadLen(byteBuf);
        ParserManager.SKIP.rdbLoadCheckModuleValue(byteBuf);
    }

    private void functionParse() {
        rdbHeader().getFunction().add(ParserManager.STRING_00.parse(byteBuf));
    }

    public RdbInfo getRdbInfo() {
        return rdbInfo;
    }

    private RdbData rdbData() {
        return rdbInfo.getRdbData();
    }

    private RdbHeader rdbHeader() {
        return rdbInfo.getRdbHeader();
    }

    private void clear() {
        rdbInfo.setDataReady(false);//数据就绪标识位置位false
        rdbInfo.setFunctionReady(false);
        rdbData().setExpiredType(ExpiredType.NONE);
        rdbData().setEvictType(EvictType.NONE);
        rdbData().setEvictValue(-1l);
        rdbData().setExpireTime(-1);
        rdbData().setRdbType(-1);
        rdbData().setKey(null);
        rdbData().setValue(null);
    }
}
