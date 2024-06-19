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

import java.nio.charset.Charset;
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
    private ByteBuf tempRdb = null;
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

            if (RdbType.START == rdbType && '$' == rdb.getByte(rdb.readerIndex())) {
                rdb.readByte();//除去$
                int index = ByteBufUtil.indexOf(Constant.SEPARAPOR, rdb);
                String isEofOrSizeStr = rdb.readBytes(index - rdb.readerIndex()).toString(CharsetUtil.UTF_8);
                logger.info("RdbType is " + rdbType.name() + ",isEofOrSizeStr " + isEofOrSizeStr);
                rdb.readBytes(Constant.SEPARAPOR.readableBytes());
                tempRdb = ByteBufAllocator.DEFAULT.buffer();
                if (isEofOrSizeStr.startsWith("EOF")) {//EOF类型看收尾
                    eofEnd = Unpooled.copiedBuffer(isEofOrSizeStr.substring(4, isEofOrSizeStr.length()).getBytes());
                    rdbType = RdbType.TYPE_EOF;
                    logger.info("RdbType is " + rdbType.name() + ",Rdb EOF is " + eofEnd.toString(Charset.defaultCharset()) + ",current data length is = " + tempRdb.readableBytes());
                } else {//LENGTH类型数长度
                    length = Integer.parseInt(isEofOrSizeStr);
                    rdbType = RdbType.TYPE_LENGTH;
                    logger.info("RdbType is " + rdbType.name() + ",RdbLength is " + length + ",current data length is = " + tempRdb.readableBytes());
                }
            }

            ByteBuf rdbBuf = null;//rdb内容
            ByteBuf commondBuf = null;//如果发生粘包，这里存放除了Rdb的后续内容

            if (rdb.readableBytes() != 0) {
                if (RdbType.TYPE_LENGTH == rdbType) {
                    tempRdb.writeBytes(rdb);
                    logger.info("RdbType is " + rdbType.name() + ",RdbLength is " + length + ",current data length is = " + tempRdb.readableBytes());
                    if (tempRdb.readableBytes() >= length) {
                        length = -1;
                        rdbType = RdbType.END;
                    }
                } else if (RdbType.TYPE_EOF == rdbType) {
                    int searchIndex = tempRdb.writerIndex() - 40;//防止拆包，从总体缓存的后40位开始检索
                    tempRdb.writeBytes(rdb);//把内容写入缓存
                    tempRdb.readerIndex(0);//只是为了打印日志
                    logger.info("RdbType is " + rdbType.name() + ",current data length is = " + tempRdb.readableBytes());
                    tempRdb.readerIndex(searchIndex);//防止拆包，从总体缓存的后40位开始检索
                    int index = ByteBufUtil.indexOf(eofEnd,tempRdb);//检索
                    if (index != -1) {
                        eofEnd = null;
                        rdbType = RdbType.END;
                        tempRdb.readerIndex(0);//读标识设置为0
                        rdbBuf = tempRdb.slice(0,index + 40);//rdb全部内容 index + 40位的eof
                        commondBuf = tempRdb.slice(index + 40,tempRdb.writerIndex());//命令内容
                    }
                }

                if (RdbType.END == rdbType) {//开始解析Rdb
                    byte[] start11 = new byte[11];
                    byte[] end11 = new byte[11];
                    if(rdbBuf.writerIndex() >= 11){
                        rdbBuf.getBytes(0,start11);
                        rdbBuf.getBytes(rdbBuf.writerIndex() - 11,end11);
                    }
                    logger.info("RdbType is " + rdbType.name() + ",Rdb prase start " + ",current data length is = " + rdbBuf.readableBytes()
                            + ",the start 11 byte is '" + new String(start11)
                            + "',the end 11 byte is '" + new String(end11) + "'");
                    rdbType = RdbType.START;
                    ctx.channel().attr(Constant.RDB_STREAM_NEXT).set(false);
                    parse(rdbBuf);
                    rdbBuf.release();
                    logger.info("The RDB stream has been processed");
                }

                if(commondBuf.isReadable()){//命令如何有内容，继续往下走
                    ctx.fireChannelRead(commondBuf);
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
