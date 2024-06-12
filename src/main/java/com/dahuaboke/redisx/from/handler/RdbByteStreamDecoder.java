package com.dahuaboke.redisx.from.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.from.RdbCommand;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.from.rdb.CommandParser;
import com.dahuaboke.redisx.from.rdb.RdbData;
import com.dahuaboke.redisx.from.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
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

    private RdbType rdbType = RdbType.START;
    private int length = -1;
    private ByteBuf eofEnd = null;
    private ByteBuf tempRdb = ByteBufAllocator.DEFAULT.buffer();
    private CommandParser commandParser = new CommandParser();
    private FromContext fromContext;

    private enum RdbType {
        START,
        TYPE_EOF,
        TYPE_LENGTH,
        END;
    }

    public RdbByteStreamDecoder(FromContext fromContext) {
        this.fromContext = fromContext;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RdbCommand) {
            ByteBuf rdb = ((RdbCommand) msg).getIn();
            logger.info("Now processing the RDB stream :" + rdbType.name());

            if (RdbType.START == rdbType && '$' == rdb.getByte(rdb.readerIndex())) {
                rdb.readByte();//除去$
                int index = ByteBufUtil.indexOf(Constant.SEPARAPOR, rdb);
                String isEofOrSizeStr = rdb.readBytes(index - rdb.readerIndex()).toString(CharsetUtil.UTF_8);
                rdb.readBytes(Constant.SEPARAPOR.readableBytes());
                if (isEofOrSizeStr.startsWith("EOF")) {//EOF类型看收尾
                    eofEnd = Unpooled.copiedBuffer(isEofOrSizeStr.substring(4, isEofOrSizeStr.length()).getBytes());
                    rdbType = RdbType.TYPE_EOF;
                } else {//LENGTH类型数长度
                    length = Integer.parseInt(isEofOrSizeStr);
                    rdbType = RdbType.TYPE_LENGTH;
                }
            }

            if (rdb.readableBytes() != 0) {
                if (RdbType.TYPE_LENGTH == rdbType) {
                    tempRdb.writeBytes(rdb);
                    if (tempRdb.readableBytes() >= length) {
                        length = -1;
                        rdbType = RdbType.END;
                    }
                } else if (RdbType.TYPE_EOF == rdbType) {
                    tempRdb.writeBytes(rdb);
                    if (ByteBufUtil.equals(eofEnd, tempRdb.slice(tempRdb.writerIndex() - eofEnd.readableBytes(), eofEnd.readableBytes()))) {
                        eofEnd = null;
                        rdbType = RdbType.END;
                    }
                }

                if (RdbType.END == rdbType) {
                    rdbType = RdbType.START;
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    parse(tempRdb);
                    tempRdb.release();
                    logger.info("The RDB stream has been processed");
                }
            }
            rdb.release();
        } else {
            ctx.fireChannelRead(msg);
        }

    }

    private void parse(ByteBuf byteBuf) {
        RdbParser parser = new RdbParser(byteBuf);
        parser.parseHeader();
        logger.debug(parser.getRdbInfo().getRdbHeader().toString());
        List<String> commands = commandParser.parser(parser.getRdbInfo().getRdbHeader());
        for (String command : commands) {
            boolean success = fromContext.publish(command);
            if (success) {
                logger.debug("Success rdb data [{}]", command);
            } else {
                logger.error("Sync rdb data [{}] failed", command);
            }
        }
        while (!parser.getRdbInfo().isEnd()) {
            parser.parseData();
            RdbData rdbData = parser.getRdbInfo().getRdbData();
            if (rdbData != null) {
                if (rdbData.getDataNum() == 1) {
                    long selectDB = rdbData.getSelectDB();
                    boolean success = fromContext.publish("select " + selectDB);
                    if (success) {
                        logger.debug("Select db success [{}]", selectDB);
                    } else {
                        logger.error("Select db failed [{}]", selectDB);
                    }
                }
                commands = commandParser.parser(rdbData);
                for (String command : commands) {
                    boolean success = fromContext.publish(command);
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
