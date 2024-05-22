package com.dahuaboke.redisx.slave.rdb;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * @Desc: RDB解析类，差分Header
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
        rdbInfo.getRdbHeader().setVer(byteBuf.readBytes(4).toString(Charset.defaultCharset()));
        boolean flag = true;
        while (flag) {
            int b = byteBuf.getByte(byteBuf.readerIndex()) & 0xff;
            switch (b) {
                //各类信息解析
                case RdbConstants.AUX:
                    readOneByte();
                    auxParse();
                    break;
                case RdbConstants.MODULE_AUX:
                    readOneByte();
                    moduleParse();
                    break;
                case RdbConstants.FUNCTION:
                    readOneByte();
                    functionParse();
                    break;
                case RdbConstants.DBSELECT:
                case RdbConstants.EOF:
                default:
                    flag = false;
                    break;
            }
        }
        logger.debug("rdbHeader message : {} ", rdbInfo.getRdbHeader());
    }

    /**
     * 解析具体数据，每次生产一条
     */
    public void parseData() {
        rdbInfo.getRdbData().clear();
        while (true) {
            int b = byteBuf.readByte() & 0xff;
            switch (b) {
                case RdbConstants.DBSELECT:
                    rdbInfo.getRdbData().setSelectDB(ParserManager.LENGTH.parse(byteBuf).len);
                    rdbInfo.getRdbData().setDataNum(1);
                    break;
                case RdbConstants.DBRESIZE:
                    rdbInfo.getRdbData().setDataCount(ParserManager.LENGTH.parse(byteBuf).len);
                    rdbInfo.getRdbData().setTtlCount(ParserManager.LENGTH.parse(byteBuf).len);
                    break;
                case RdbConstants.EXPIRED_FC:
                    rdbInfo.getRdbData().setExpireTime(byteBuf.readLongLE());
                    break;
                case RdbConstants.EXPIRED_FD:
                    rdbInfo.getRdbData().setExpireTime(byteBuf.readIntLE());
                    break;
                case RdbConstants.EOF:
                    rdbInfo.setEnd(true);
                    rdbInfo.setRdbData(null);
                    return;
                default:
                    rdbInfo.getRdbData().setRdbType(b);
                    rdbInfo.getRdbData().setKey(ParserManager.STRING_00.parse(byteBuf));
                    rdbInfo.getRdbData().setValue(ParserManager.getParser(b).parse(byteBuf));
                    return;
            }
        }
    }

    private void auxParse() {
        String key = new String(ParserManager.STRING_00.parse(byteBuf));
        String value = new String(ParserManager.STRING_00.parse(byteBuf));
        switch (key) {
            case "redis-ver":
                rdbInfo.getRdbHeader().setRedisVer(value);
                return;
            case "redis-bits":
                rdbInfo.getRdbHeader().setRedisBits(value);
                return;
            case "ctime":
                rdbInfo.getRdbHeader().setCtime(value);
                return;
            case "used-mem":
                rdbInfo.getRdbHeader().setUsedMem(value);
                return;
            case "repl-stream-db":
                rdbInfo.getRdbHeader().setReplStreamDb(value);
                return;
            case "repl-id":
                rdbInfo.getRdbHeader().setReplId(value);
                return;
            case "repl-offset":
                rdbInfo.getRdbHeader().setReplOffset(value);
                return;
            case "aof-base":
                rdbInfo.getRdbHeader().setAofBase(value);
                return;
            default:
                return;
        }
    }

    private void moduleParse() {
        //rdbHeader.set();
    }

    private void functionParse() {
        String value = new String(ParserManager.STRING_00.parse(byteBuf));
        rdbInfo.getRdbHeader().getFunction().add(value);
    }

    private void readOneByte() {
        byteBuf.readByte();
    }

    public RdbInfo getRdbInfo() {
        return rdbInfo;
    }
}
