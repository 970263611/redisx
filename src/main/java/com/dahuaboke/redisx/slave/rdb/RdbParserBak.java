package com.dahuaboke.redisx.slave.rdb;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * 2024/5/16 16:06
 * auth: dahua
 * desc:
 */
public class RdbParserBak {

    private static final Logger logger = LoggerFactory.getLogger(RdbParserBak.class);
    private static final Charset UTF8 = CharsetUtil.UTF_8;

    public void parse(ByteBuf rdb) {
        //REDIS magic
        ByteBuf magicByte = rdb.readBytes(5);
        String magic = magicByte.toString(UTF8);
        //版本
        ByteBuf versionByte = rdb.readBytes(4);
        logger.debug("Rdb version {}", versionByte.toString(UTF8));
//        if (RDB_MAGIC.equals(magic)) {
//            while (rdb.isReadable()) {
//                int type = rdb.readByte() & 0xff;
//                switch (type) {
//                    //过期时间秒
//                    case RDB_OPCODE_EXPIRETIME:
//                        //过期时间长度4位
//                        int exLength = rdb.readInt();
//                        ByteBuf exBuf = rdb.readBytes(exLength);
//                        byte sonType = exBuf.readByte();
//                        continue;
//                }
//            }
//        } else {
//            logger.error("Rdb format error, magic is {}", magic);
//        }
    }
}
