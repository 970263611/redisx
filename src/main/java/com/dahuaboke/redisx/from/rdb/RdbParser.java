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
        this.rdbInfo = new RdbInfo(byteBuf);
        this.byteBuf = byteBuf;
    }

    /**
     * 获取文件描述信息
     */
    public void parseHeader() {
        //必须已REDIS开头
        if (!RdbConstants.START.equals(byteBuf.readBytes(5).toString(Charset.defaultCharset()))) {
            return;
        }
        //后面四位是版本
        rdbHeader().setVer(byteBuf.readBytes(4).toString(Charset.defaultCharset()));
        boolean flag = true;
        while (flag) {
            byteBuf.markReaderIndex();
            int b = byteBuf.readByte() & 0xff;
            switch (b) {
                //各类信息解析
                case RdbConstants.AUX:
                    auxParse();
                    break;
                case RdbConstants.MODULE_AUX:
                    moduleParse();
                    break;
                case RdbConstants.FUNCTION:
                    functionParse();
                    break;
                case RdbConstants.DBSELECT:
                    byteBuf.resetReaderIndex();
                    flag = false;
                    break;
                case RdbConstants.EOF:
                    rdbInfo.setEnd(true);
                    rdbInfo.setRdbData(null);
                default:
                    int index = ByteBufUtil.indexOf(Unpooled.copiedBuffer(new byte[]{(byte) 0xfb}), byteBuf);
                    if (index != -1) {
                        if (index >= byteBuf.readerIndex()) {
                            byteBuf.readBytes(index - byteBuf.readerIndex());
                            continue;
                        }
                    }
                    index = ByteBufUtil.indexOf(Unpooled.copiedBuffer(new byte[]{(byte) 0xff}), byteBuf);
                    if (index != -1) {
                        if (index >= byteBuf.readerIndex()) {
                            byteBuf.readBytes(index - byteBuf.readerIndex());
                            continue;
                        }
                    }
                    flag = false;
            }
        }
    }

    /**
     * 解析具体数据，每次生产一条
     */
    public void parseData() {
        clear();
        while (true) {
            int b = byteBuf.readByte() & 0xff;
            switch (b) {
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
                case RdbConstants.EOF:
                    rdbInfo.setEnd(true);
                    rdbInfo.setRdbData(null);
                    return;
                default:
                    rdbData().setRdbType(b);
                    rdbData().setKey(ParserManager.STRING_00.parse(byteBuf));
                    Parser parser = ParserManager.getParser(b);
                    if (parser != null) {
                        rdbData().setValue(parser.parse(byteBuf));
                    }
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

    private void readOneByte() {
        byteBuf.readByte();
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
        rdbData().setExpiredType(ExpiredType.NONE);
        rdbData().setExpireTime(-1);
        rdbData().setDataNum(rdbData().getDataNum() + 1);
        rdbData().setRdbType(-1);
        rdbData().setKey(null);
        rdbData().setValue(null);
        rdbData().setEvictType(EvictType.NONE);
        rdbData().setEvictValue(-1l);
    }
}
