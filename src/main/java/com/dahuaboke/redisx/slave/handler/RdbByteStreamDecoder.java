package com.dahuaboke.redisx.slave.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.slave.RdbCommand;
import com.dahuaboke.redisx.slave.SlaveContext;
import com.dahuaboke.redisx.slave.rdb.CommandParser;
import com.dahuaboke.redisx.slave.rdb.RdbData;
import com.dahuaboke.redisx.slave.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 2024/5/6 11:09
 * auth: dahua
 * desc: Rdb文件解析处理类
 */
public class RdbByteStreamDecoder extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RdbByteStreamDecoder.class);
    private String eofEnd = null;
    private boolean rdbEnd = false;
    private ByteBuf tempRdb = ByteBufAllocator.DEFAULT.buffer();
    private CommandParser commandParser = new CommandParser();
    private SlaveContext slaveContext;

    public RdbByteStreamDecoder(SlaveContext slaveContext) {
        this.slaveContext = slaveContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            RdbCommand command = (RdbCommand) msg;
            logger.info("Now processing the RDB stream");
            ByteBuf rdb = command.getIn();
            int length = rdb.readableBytes();
            if ('$' == rdb.getByte(0) && !rdbEnd) {
                int index = 0;
                while (rdb.isReadable()) {
                    byte b = rdb.getByte(index);
                    if (b == '\r') {
                        break;
                    }
                    if (b != '\r') {
                        index++;
                    }
                }
                String isEofOrSizeStr = rdb.slice(1, index - 1).toString(CharsetUtil.UTF_8);
                if (isEofOrSizeStr.startsWith("EOF")) {
                    //7.X $EOF:40位\r\n
                    rdb.readBytes(47);
                    eofEnd = isEofOrSizeStr.split(":")[1];
                    if (length > 40) {
                        String end = rdb.slice(length - 40, 40).toString(CharsetUtil.UTF_8);
                        if (eofEnd.equals(end)) {
                            rdbEnd = true;
                        } else {
                            tempRdb = Unpooled.copiedBuffer(tempRdb, rdb);
                        }
                    } else {
                        tempRdb = Unpooled.copiedBuffer(tempRdb, rdb);
                    }
                } else {
                    //$xxx\r\n
                    rdb.readBytes(isEofOrSizeStr.length() + 3);
                    if (rdb.readableBytes() == Integer.parseInt(isEofOrSizeStr)) {
                        rdbEnd = true;
                        tempRdb = Unpooled.copiedBuffer(tempRdb, rdb);
                    }
                }
            } else if (!rdbEnd) {
                if (length > 40) {
                    String end = rdb.slice(length - 40, 40).toString(CharsetUtil.UTF_8);
                    if (eofEnd.equals(end)) {
                        rdbEnd = true;
                    }
                }
                tempRdb = Unpooled.copiedBuffer(tempRdb, rdb);
            }
            if (rdbEnd) {
                rdbEnd = false;
                ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                parse(tempRdb);
                tempRdb = ByteBufAllocator.DEFAULT.buffer();
                logger.info("The RDB stream has been processed");
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
                List<String> commands = commandParser.parser(rdbData);
                for (String command : commands) {
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
}
