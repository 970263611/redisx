package com.dahuaboke.redisx.slave.rdb;

import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * @Desc: RDB解析类，差分Header
 * @Author：cdl
 * @Date：2024/5/20 14:01
 */
public class RdbInfoParser {

    private ByteBuf byteBuf;

    private RdbFileInfo info;

    public RdbInfoParser(ByteBuf byteBuf){
        this.byteBuf = byteBuf;
        this.info = new RdbFileInfo();
    }

    public void parse() throws Exception {
        //必须已REDIS开头
        if(!RdbConstants.START.equals(byteBuf.readBytes(5).toString(Charset.defaultCharset()))){
            return;
        }
        //后面四位是版本
        info.setVer(byteBuf.readBytes(4).toString(Charset.defaultCharset()));
        boolean flag = true;
        while (flag){
            int sign = byteBuf.readByte() & 0xff;
            switch (sign){
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
                    dbselectParse();
                    break;
                case RdbConstants.EOF:
                    flag = false;
                    break;
                default:
                    flag = false;
                    break;
            }
        }
        System.out.println(info);
    }

    private void auxParse() throws Exception {
        String key = new String(ParserManager.STRING_00.parse(byteBuf));
        String value = new String(ParserManager.STRING_00.parse(byteBuf));
        switch (key){
            case "redis-ver":
                info.setRedisVer(value);
                return;
            case "redis-bits":
                info.setRedisBits(value);
                return;
            case "ctime":
                info.setCtime(value);
                return;
            case "used-mem":
                info.setUsedMem(value);
                return;
            case "repl-stream-db":
                info.setReplStreamDb(value);
                return;
            case "repl-id":
                info.setReplId(value);
                return;
            case "repl-offset":
                info.setReplOffset(value);
                return;
            case "aof-base":
                info.setAofBase(value);
                return;
            default:
                return;
        }
    }

    private void moduleParse(){

    }

    private void functionParse() throws Exception {
        String value = new String(ParserManager.STRING_00.parse(byteBuf));
        info.getFunction().add(value);
    }

    private void dbselectParse(){

    }

}
