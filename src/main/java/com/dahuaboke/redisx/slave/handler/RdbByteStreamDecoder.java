package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import com.dahuaboke.redisx.slave.SlaveContext;
import com.dahuaboke.redisx.slave.rdb.CommandParser;
import com.dahuaboke.redisx.slave.rdb.RdbData;
import com.dahuaboke.redisx.slave.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);
    private int rdbSize = 0;
    private CommandParser commandParser = new CommandParser();
    private SlaveContext slaveContext;

    public RdbByteStreamDecoder(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            RdbCommand rdb = (RdbCommand) msg;
            logger.info("Now processing the RDB stream");
            ByteBuf in = rdb.getIn();
            try {
                //设置标记索引，防止多次暂存，后面利用浅拷贝
                int index = 0;
                while (in.isReadable()) {
                    byte b = in.readByte();
                    if (b == '\n') {
                        break;
                    }
                    if (b != '\r') {
                        index++;
                    }
                }
                if (index > 0) {
                    if ('$' == in.getByte(0)) {
                        ByteBuf tempBuf = in.slice(1, index - 1);
                        if ("EOF".equals(tempBuf.slice(0, 3).toString(CharsetUtil.UTF_8).trim())) {
                            //redis 7.X
                            String eof = tempBuf.readBytes(44).toString(CharsetUtil.UTF_8);
                            System.out.println(eof);
                            if (in.isReadable()) {
                                //证明rdb流是一起过来的
                                parse(in);
                                logger.info("The RDB stream has been processed");
                                ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                            }
                        } else {
                            String rdbSizeCommand = tempBuf.toString(CharsetUtil.UTF_8);
                            rdbSize = Integer.parseInt(rdbSizeCommand);
                            if (in.readableBytes() == rdbSize) {
                                //index + 2 跳过\r\n
                                ByteBuf rdbStream = in.slice(index + 2, rdbSize);
                                parse(rdbStream);
                                logger.info("The RDB stream has been processed");
                                ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                            }
                        }
                        //else 流是分开的，需要等下一次处理
                    } else if ('R' == in.getByte(0)) {
                        ByteBuf rdbStream = in.slice(0, rdbSize);
                        parse(rdbStream);
                        logger.info("The RDB stream has been processed");
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    } else {
                        //无法识别流
                        logger.error("Unknown RDB stream format");
                        ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    }
                }
                //else 空指令跳过
            } finally {
                in.release();
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void parse(ByteBuf byteBuf) {
        RdbParser parser = new RdbParser(byteBuf);
        parser.parseHeader();
        System.out.println(parser.getRdbInfo().getRdbHeader());
        while (!parser.getRdbInfo().isEnd()) {
            parser.parseData();
            RdbData rdbData = parser.getRdbInfo().getRdbData();
            if (rdbData != null) {
                if (rdbData.getDataNum() == 1) {
                    long selectDB = rdbData.getSelectDB();
                    boolean success = slaveContext.publish("select " + selectDB);
                    if (success) {
                        logger.debug("Select db success [{}]", selectDB);
                    } else {
                        logger.error("Select db failed [{}]", selectDB);
                    }
                }
                String command = commandParser.parser(rdbData);
                boolean success = slaveContext.publish(command);
                if (success) {
                    logger.debug("Success rdb data [{}]", command);
                } else {
                    logger.error("Sync rdb data [{}] failed", command);
                }
            }
        }
    }
}
